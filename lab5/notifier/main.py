import asyncio
import json
import os
import random
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://loyalty:loyalty_pass@rabbitmq/")
MIN_DELAY_MS = int(os.getenv("MIN_DELAY_MS", "200"))
MAX_DELAY_MS = int(os.getenv("MAX_DELAY_MS", "1500"))

UNSUBSCRIBED = {
    item.strip()
    for item in os.getenv("UNSUBSCRIBED", "").split(",")
    if item.strip()
}

notifications = []
events_received = 0
notifications_sent = 0
consumer_task = None
connection = None


def create_notification(event: dict) -> str:
    if event["status"] == "rejected":
        return f"Операция отклонена: {event['reject_reason']}"

    delta = int(event["points_delta"])
    balance = event["balance_after"]

    if delta > 0:
        return f"Начислено {delta} баллов, баланс {balance}"

    if delta < 0:
        return f"Списано {abs(delta)} баллов, баланс {balance}"

    return f"Операция выполнена, баланс {balance}"


async def consume_events():
    global events_received, notifications_sent

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(
        "loyalty.events",
        aio_pika.ExchangeType.FANOUT,
        durable=True
    )

    queue = await channel.declare_queue(
        "notifier.events",
        durable=True,
        arguments={"x-queue-type": "quorum"}
    )
    await queue.bind(exchange)

    async def handle_message(message: aio_pika.IncomingMessage):
        global events_received, notifications_sent

        async with message.process():
            event = json.loads(message.body.decode("utf-8"))
            events_received += 1

            customer_id = event["customer_id"]

            if customer_id in UNSUBSCRIBED:
                print(f"Покупатель {customer_id} отказался от уведомлений")
                return

            delay = random.randint(MIN_DELAY_MS, MAX_DELAY_MS) / 1000
            await asyncio.sleep(delay)

            notification = {
                "operation_id": event["operation_id"],
                "customer_id": customer_id,
                "store_id": event["store_id"],
                "message": create_notification(event),
                "created_at": event["processed_at"]
            }

            notifications.append(notification)
            notifications_sent += 1

            print(f"Уведомление для {customer_id}: {notification['message']}")

    await queue.consume(handle_message)
    await asyncio.Future()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global connection, consumer_task

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    consumer_task = asyncio.create_task(consume_events())

    yield

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    if connection and not connection.is_closed:
        await connection.close()


app = FastAPI(
    title="Loyalty Notifier",
    description="Сервис уведомлений покупателей",
    lifespan=lifespan
)


@app.get("/notifications")
async def get_notifications(customer_id: str | None = None):
    if customer_id is None:
        return notifications

    return [
        item for item in notifications
        if item["customer_id"] == customer_id
    ]


@app.get("/stats")
async def get_stats():
    return {
        "events_received": events_received,
        "notifications_sent": notifications_sent
    }


@app.get("/health")
async def health():
    if connection is None or connection.is_closed:
        return {"status": "unavailable"}
    return {"status": "ok"}
