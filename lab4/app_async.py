
"""Версия 3: полностью асинхронные запросы, вычисления в основном потоке."""

import asyncio
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
    # Он используется для запросов ко всем поставщикам.
    app.state.client = httpx.AsyncClient(
        timeout=None,
        limits=httpx.Limits(max_connections=None)
    )

    # Создаем асинхронный движок для подключения к PostgreSQL.
    app.state.engine = create_async_engine(
        DB_URL.replace("+psycopg", "+asyncpg"),
        pool_size=20
    )

    yield

    # Закрываем HTTP-клиент и движок БД при остановке приложения.
    await app.state.client.aclose()
    await app.state.engine.dispose()


app = FastAPI(lifespan=lifespan)


async def load_own(engine, day: Date):
    """Асинхронно загружает рейсы из собственной базы данных."""
    async with engine.connect() as conn:
        result = await conn.execute(day_query(day))
        return [Flight(*row) for row in result]


async def fetch(client: httpx.AsyncClient, name: str, day: Date):
    """Асинхронно получает рейсы поставщика с повтором при ошибке."""
    try:
        # Общее ограничение времени включает запрос, паузу и повтор.
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
    # Запоминаем начало обработки запроса и получаем ресурсы приложения.
    t0 = perf_counter()
    st = request.app.state

    # Параллельно получаем данные из своей БД и трех поставщиков.
    # Ошибка одного поставщика не прерывает запросы к остальным.
    res = await asyncio.gather(
        load_own(st.engine, date),
        *(fetch(st.client, p, date) for p in PROVIDERS),
        return_exceptions=True
    )

    # Если произошла ошибка при загрузке данных из собственной БД,
    # передаем ее для дальнейшей обработки.
    if isinstance(res[0], BaseException):
        raise res[0]

    # Формируем словарь результатов от поставщиков.
    # Если отдельный поставщик завершился ошибкой, вместо его данных
    # записываем None.
    got = {
        p: None if isinstance(r, BaseException) else r
        for p, r in zip(PROVIDERS, res[1:])
    }

    # Выполняем обработку рейсов и построение маршрутов
    # непосредственно в основном потоке событий.
    return answer(res[0], got, origin, destination, t0)


@app.get("/health")
async def health():
    """Проверяет доступность сервиса."""
    return {"status": "ok"}
