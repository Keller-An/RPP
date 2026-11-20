"""Заглушка поставщика авиарейсов.
Параметры задаются переменными окружения согласно таблице 1 методички.
"""
import asyncio
import json
import os
import random
from datetime import date as Date
from functools import lru_cache

from fastapi import FastAPI, HTTPException, Response

from common import gen_flights

NAME = os.environ["PROVIDER_NAME"]
DELAY = float(os.getenv("DELAY_MS", 300)) / 1000
ERROR = float(os.getenv("ERROR_PCT", 0)) / 100
SLOW = float(os.getenv("SLOW_PCT", 0)) / 100
COUNT = int(os.getenv("FLIGHTS", 500))

app = FastAPI()


@lru_cache(maxsize=None)
def day_json(day: Date) -> bytes:
    """Для одной даты одного поставщика рейсы всегда одинаковые."""
    return json.dumps(
        [f._asdict() for f in gen_flights(NAME, day, COUNT)],
        default=str
    ).encode()


@app.get("/flights")
async def flights(date: Date):
    """Сначала задержка (как у реального поставщика), затем ошибка или ответ."""

    # Долгий ответ (3 с) — с вероятностью SLOW
    if random.random() < SLOW:
        await asyncio.sleep(3)
    else:
        # Обычная задержка ± 30 %
        await asyncio.sleep(random.uniform(0.7, 1.3) * DELAY)

    # Ответ с ошибкой 500 — с вероятностью ERROR
    if random.random() < ERROR:
        raise HTTPException(status_code=500, detail="provider error")

    return Response(
        day_json(date),
        media_type="application/json"
    )


@app.get("/health")
async def health():
    return {"status": "ok"}