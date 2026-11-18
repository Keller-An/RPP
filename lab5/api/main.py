import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Literal

import aio_pika
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, model_validator

from db import check_database, engine, get_account, get_operation

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://loyalty:loyalty_pass@rabbitmq/"
)

connection = None
channel = None
exchange = None


class OperationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_id: str = Field(min_length=1)
    type: Literal["purchase", "return", "redeem"]
    customer_id: str = Field(min_length=1)
    store_id: str = Field(min_length=1)
    check_number: str = Field(min_length=1)
    occurred_at: datetime
    amount: Decimal | None = Field(default=None, gt=0)
    points: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_operation(self):
        if self.type in ("purchase", "redeem"):
            if self.amount is None:
                raise ValueError("Для покупки и списания нужна сумма amount")
        elif self.amount is not None:
            raise ValueError("Для возврата поле amount не указывается")

        if self.type == "redeem":
            if self.points is None:
                raise ValueError("Для списания необходимо поле points")
        elif self.points is not None:
            raise ValueError("Поле points указывается только для redeem")

        return self


@asynccontextmanager
async def lifespan(app: FastAPI):
    global connection, channel, exchange

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel(publisher_confirms=True)

    exchange = await channel.declare_exchange(
        "loyalty",
        aio_pika.ExchangeType.DIRECT,
        durable=True
    )

    yield

    if connection and not connection.is_closed:
        await connection.close()
    await engine.dispose()


app = FastAPI(
    title="Loyalty API",
    description="API бонусной программы магазинов",
    version="1.0.0",
    lifespan=lifespan
)


@app.post("/operations", status_code=status.HTTP_202_ACCEPTED)
async def create_operation(operation: OperationRequest):
    message = aio_pika.Message(
        body=json.dumps(
            operation.model_dump(mode="json"),
            ensure_ascii=False
        ).encode("utf-8"),
        message_id=operation.operation_id,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
    )

    try:
        confirmation = await exchange.publish(
            message,
            routing_key="operation"
        )
        if confirmation is False:
            raise RuntimeError("RabbitMQ не подтвердил публикацию")
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Не удалось отправить операцию в очередь: {exc}"
        )

    return {"operation_id": operation.operation_id}


@app.get("/operations/{operation_id}")
async def read_operation(operation_id: str):
    try:
        operation = await get_operation(operation_id)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="База данных временно недоступна"
        )

    if operation is None:
        raise HTTPException(status_code=404, detail="Операция ещё не обработана")

    return {
        "operation_id": operation["operation_id"],
        "status": operation["status"],
        "points_delta": operation["points_delta"],
        "balance_after": str(operation["balance_after"]),
        "reject_reason": operation["reject_reason"]
    }


@app.get("/accounts/{customer_id}")
async def read_account(customer_id: str):
    try:
        result = await get_account(customer_id)
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="База данных временно недоступна"
        )

    if result is None:
        raise HTTPException(status_code=404, detail="Покупатель не найден")

    account, operations = result

    return {
        "customer_id": account["customer_id"],
        "balance": str(account["balance"]),
        "operations": [
            {
                "operation_id": item["operation_id"],
                "type": item["type"],
                "store_id": item["store_id"],
                "check_number": item["check_number"],
                "amount": str(item["amount"]) if item["amount"] is not None else None,
                "points": item["points"],
                "points_delta": item["points_delta"],
                "balance_after": str(item["balance_after"]),
                "status": item["status"],
                "reject_reason": item["reject_reason"],
                "occurred_at": item["occurred_at"].isoformat(),
                "processed_at": item["processed_at"].isoformat()
            }
            for item in operations
        ]
    }


@app.get("/health")
async def health():
    try:
        await check_database()
        if connection is None or connection.is_closed:
            raise RuntimeError("RabbitMQ connection is closed")
        return {"status": "ok", "database": "ok", "rabbitmq": "ok"}
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Сервис недоступен: {exc}"
        )
