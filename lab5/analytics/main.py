import asyncio
import json
import os
from collections import defaultdict
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, HTTPException

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://loyalty:loyalty_pass@rabbitmq/")

store_stats = {}
events_received = 0
consumer_task = None
connection = None


def get_empty_stats():
    return {
        "operations": {
            "purchase": 0,
            "return": 0,
            "redeem": 0
        },
        "rejected_operations": 0,
        "points_earned": 0,
        "points_removed_on_returns": 0,
        "points_spent_on_purchases": 0
    }


async def consume_events():
    global events_received

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(
        "loyalty.events",
        aio_pika.ExchangeType.FANOUT,
        durable=True
    )

    queue = await channel.declare_queue(
        "analytics.events",
        durable=True,
        arguments={"x-queue-type": "quorum"}
    )
    await queue.bind(exchange)

    async def handle_message(message: aio_pika.IncomingMessage):
        global events_received

        async with message.process():
            event = json.loads(message.body.decode("utf-8"))
            events_received += 1

            store_id = event["store_id"]

            if store_id not in store_stats:
                store_stats[store_id] = get_empty_stats()

            stats = store_stats[store_id]

            if event["status"] == "rejected":
                stats["rejected_operations"] += 1
                return

            operation_type = event["type"]
            delta = int(event["points_delta"])

            stats["operations"][operation_type] += 1

            if operation_type == "purchase":
                stats["points_earned"] += max(delta, 0)

            elif operation_type == "return":
                stats["points_removed_on_returns"] += abs(delta)

            elif operation_type == "redeem":
                stats["points_spent_on_purchases"] += abs(delta)

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
    title="Loyalty Analytics",
    description="Аналитика бонусной программы по магазинам",
    lifespan=lifespan
)


@app.get("/stores")
async def get_stores():
    return [
        {"store_id": store_id, **stats}
        for store_id, stats in store_stats.items()
    ]


@app.get("/stores/{store_id}")
async def get_store(store_id: str):
    if store_id not in store_stats:
        raise HTTPException(status_code=404, detail="Магазин не найден")

    return {
        "store_id": store_id,
        **store_stats[store_id]
    }


@app.get("/stats")
async def get_stats():
    return {"events_received": events_received}


@app.get("/health")
async def health():
    if connection is None or connection.is_closed:
        return {"status": "unavailable"}
    return {"status": "ok"}
