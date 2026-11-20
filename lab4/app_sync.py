"""Версия 1 (задача 3.3): синхронная.
Поиск и /health выполняются через def, поставщики опрашиваются по очереди.
"""

import time
from datetime import date as Date
from time import perf_counter

import requests
from fastapi import FastAPI
from sqlalchemy import create_engine

from common import Flight
from core import (
    BUDGET,
    DB_URL,
    PROVIDERS,
    RETRY_PAUSE,
    answer,
    day_query,
)

app = FastAPI()

engine = create_engine(
    DB_URL,
    pool_size=20
)


def fetch(name: str, day: Date):
    """Получает рейсы поставщика или возвращает None при ошибке."""

    deadline = time.monotonic() + BUDGET

    for attempt in (1, 2):
        remaining = deadline - time.monotonic()

        if remaining <= 0:
            return None

        try:
            response = requests.get(
                f"http://{name}:8000/flights",
                params={"date": day},
                timeout=remaining
            )

            response.raise_for_status()
            return response.json()

        except requests.Timeout:
            # Поставщик не ответил в отведённое время — рейсы не включаются.
            return None

        except requests.RequestException:
            # Одна повторная попытка после ошибки.
            if attempt == 2:
                return None

            if deadline - time.monotonic() <= RETRY_PAUSE:
                return None

            time.sleep(RETRY_PAUSE)

    return None


@app.get("/search")
def search(origin: str, destination: str, date: Date):
    """Синхронный поиск рейсов."""

    t0 = perf_counter()

    with engine.connect() as conn:
        own = [
            Flight(*row)
            for row in conn.execute(day_query(date))
        ]

    # Поставщики опрашиваются последовательно.
    got = {
        provider: fetch(provider, date)
        for provider in PROVIDERS
    }

    return answer(own, got, origin, destination, t0)


@app.get("/health")
def health():
    """Синхронная проверка: объявлена через def, как и поиск (требование 3.3)."""

    return {"status": "ok"}