import asyncio
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import aio_pika
from sqlalchemy import select

from db import Account, Operation, SessionLocal, create_tables, engine
from domain import validate_operation

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://loyalty:loyalty_pass@rabbitmq/")


def make_event(operation):
    return {
        "operation_id": operation.operation_id,
        "type": operation.type,
        "customer_id": operation.customer_id,
        "store_id": operation.store_id,
        "status": operation.status,
        "points_delta": operation.points_delta,
        "balance_after": str(operation.balance_after),
        "reject_reason": operation.reject_reason,
        "processed_at": operation.processed_at.isoformat()
    }


async def process_operation(data: dict) -> dict:
    """Обрабатывает операцию в одной транзакции PostgreSQL."""

    async with SessionLocal() as session:
        async with session.begin():
            existing = await session.get(Operation, data["operation_id"])

            # При повторной доставке возвращаем ранее сохранённый результат.
            if existing is not None:
                return make_event(existing)

            customer_id = data["customer_id"]

            await session.execute(
                select(Account).where(Account.customer_id == customer_id)
            )

            account = await session.get(Account, customer_id)

            if account is None:
                account = Account(
                    customer_id=customer_id,
                    balance=Decimal("0")
                )
                session.add(account)
                await session.flush()

            # Блокируем строку счёта на время изменения баланса.
            account = (
                await session.execute(
                    select(Account)
                    .where(Account.customer_id == customer_id)
                    .with_for_update()
                )
            ).scalar_one()

            operation_type = data["type"]
            amount = Decimal(str(data["amount"])) if data.get("amount") is not None else None
            points = data.get("points")

            purchase = None
            already_returned = False

            if operation_type == "purchase":
                duplicate = (
                    await session.execute(
                        select(Operation).where(
                            Operation.type == "purchase",
                            Operation.check_number == data["check_number"],
                            Operation.status == "applied"
                        )
                    )
                ).scalar_one_or_none()

                if duplicate is not None:
                    reason = "duplicate_purchase"
                    delta = 0
                else:
                    reason, delta = validate_operation(
                        operation_type, amount, points, account.balance
                    )

            elif operation_type == "return":
                purchase_operation = (
                    await session.execute(
                        select(Operation).where(
                            Operation.type == "purchase",
                            Operation.check_number == data["check_number"],
                            Operation.status == "applied"
                        )
                    )
                ).scalar_one_or_none()

                if purchase_operation is not None:
                    purchase = {
                        "points_delta": purchase_operation.points_delta,
                        "customer_id": purchase_operation.customer_id
                    }

                if purchase is None:
                    reason, delta = "purchase_not_found", 0
                elif purchase["customer_id"] != customer_id:
                    reason, delta = "customer_mismatch", 0
                else:
                    returned = (
                        await session.execute(
                            select(Operation).where(
                                Operation.type == "return",
                                Operation.check_number == data["check_number"],
                                Operation.status == "applied"
                            )
                        )
                    ).scalar_one_or_none()

                    already_returned = returned is not None
                    reason, delta = validate_operation(
                        operation_type, amount, points, account.balance,
                        purchase, already_returned
                    )

            else:
                reason, delta = validate_operation(
                    operation_type, amount, points, account.balance
                )

            if reason is None:
                operation_status = "applied"
                account.balance += Decimal(delta)
            else:
                operation_status = "rejected"
                delta = 0

            now = datetime.now(timezone.utc)

            operation = Operation(
                operation_id=data["operation_id"],
                type=operation_type,
                customer_id=customer_id,
                store_id=data["store_id"],
                check_number=data["check_number"],
                amount=amount,
                points=points,
                points_delta=delta,
                balance_after=account.balance,
                status=operation_status,
                reject_reason=reason,
                occurred_at=datetime.fromisoformat(
                    data["occurred_at"].replace("Z", "+00:00")
                ),
                processed_at=now
            )

            session.add(operation)
            await session.flush()

            event = make_event(operation)

        # Выход из session.begin означает, что транзакция зафиксирована.
        return event


async def main():
    await create_tables()
    print("Worker: таблицы PostgreSQL готовы")

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=1)

    # Очередь входящих операций: direct exchange.
    direct_exchange = await channel.declare_exchange(
        "loyalty",
        aio_pika.ExchangeType.DIRECT,
        durable=True
    )

    operations_queue = await channel.declare_queue(
        "operations",
        durable=True,
        arguments={"x-queue-type": "quorum"}
    )
    await operations_queue.bind(direct_exchange, routing_key="operation")

    # События для независимых подписчиков: fanout exchange.
    events_exchange = await channel.declare_exchange(
        "loyalty.events",
        aio_pika.ExchangeType.FANOUT,
        durable=True
    )

    notifier_queue = await channel.declare_queue(
        "notifier.events",
        durable=True,
        arguments={"x-queue-type": "quorum"}
    )
    analytics_queue = await channel.declare_queue(
        "analytics.events",
        durable=True,
        arguments={"x-queue-type": "quorum"}
    )

    await notifier_queue.bind(events_exchange)
    await analytics_queue.bind(events_exchange)

    async def handle_message(message: aio_pika.IncomingMessage):
        async with message.process(requeue=True):
            data = json.loads(message.body.decode("utf-8"))

            # Сначала сохраняем результат в БД.
            event = await process_operation(data)

            # Затем публикуем событие. ACK входящего сообщения произойдёт
            # только после подтверждения публикации.
            outgoing = aio_pika.Message(
                body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                message_id=event["operation_id"],
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            confirmation = await events_exchange.publish(
                outgoing,
                routing_key=""
            )

            if confirmation is False:
                raise RuntimeError("RabbitMQ не подтвердил публикацию события")

            print(
                f"Обработана операция {event['operation_id']}: "
                f"{event['status']}, изменение баланса {event['points_delta']}"
            )

    await operations_queue.consume(handle_message)
    print("Worker запущен. Ожидание операций...")

    try:
        await asyncio.Future()
    finally:
        await connection.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
