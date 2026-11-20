
"""Версия 4: асинхронный сбор данных с выносом вычислений в отдельный поток."""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import date as Date
from time import perf_counter

import httpx
from fastapi import FastAPI, Request
from sqlalchemy.ext.asyncio import create_async_engine

from common import Flight
from core import BUDGET, DB_URL, PROVIDERS, RETRY_PAUSE, answer, day_query


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Создаем асинхронный HTTP-клиент при запуске приложения.
    # Он будет использоваться для запросов ко всем поставщикам.
    app.state.client = httpx.AsyncClient(
        timeout=None,
        limits=httpx.Limits(max_connections=None)
    )

    # Создаем асинхронный движок для работы с PostgreSQL.
    app.state.engine = create_async_engine(
        DB_URL.replace("+psycopg", "+asyncpg"),
        pool_size=20
    )

    yield

    # Закрываем HTTP-клиент и соединения с БД при завершении приложения.
    await app.state.client.aclose()
    await app.state.engine.dispose()


app = FastAPI(lifespan=lifespan)


async def load_own(engine, day: Date):
    """Асинхронное получение рейсов из собственной базы данных."""
    async with engine.connect() as conn:
        result = await conn.execute(day_query(day))
        return [Flight(*row) for row in result]


async def fetch(client: httpx.AsyncClient, name: str, day: Date):
    """Получение рейсов поставщика с повторным запросом при ошибке."""
    try:
        # Ограничиваем общее время: первый запрос, пауза и повтор.
        async with asyncio.timeout(BUDGET):
            for attempt in (1, 2):
                try:
                    response = await client.get(
                        f"http://{name}:8000/flights",
                        params={"date": day}
                    )
                    response.raise_for_status()
                    return response.json()

                except httpx.HTTPError:
                    if attempt == 2:
                        return None

                    # Асинхронная пауза перед повторным запросом.
                    await asyncio.sleep(RETRY_PAUSE)

    except TimeoutError:
        return None


@app.get("/search")
async def search(
    request: Request,
    origin: str,
    destination: str,
    date: Date
):
    t0 = perf_counter()
    st = request.app.state

    # Одновременно запрашиваем собственную БД и трех поставщиков.
    results = await asyncio.gather(
        load_own(st.engine, date),
        *(fetch(st.client, provider, date) for provider in PROVIDERS),
        return_exceptions=True
    )

    # Если произошла ошибка чтения собственной БД,
    # передаем ее дальше для обработки.
    if isinstance(results[0], BaseException):
        raise results[0]

    # Отделяем результаты поставщиков и учитываем возможные ошибки.
    got = {
        provider: None if isinstance(result, BaseException) else result
        for provider, result in zip(PROVIDERS, results[1:])
    }

    # Вычислительную часть переносим в отдельный поток.
    # Это позволяет основному циклу событий продолжать обслуживать
    # другие запросы, в том числе GET /health.
    return await asyncio.to_thread(
        answer,
        results[0],
        got,
        origin,
        destination,
        t0
    )


@app.get("/health")
async def health():
    """Проверка работоспособности сервиса."""
    return {"status": "ok"}
