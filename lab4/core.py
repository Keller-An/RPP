"""Общая часть трёх версий сервиса поиска."""
import os
import time
from datetime import date, datetime, time as dtime, timedelta

from fastapi import HTTPException
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, select

from common import Flight, build_routes

PROVIDERS = os.getenv("PROVIDERS", "p1,p2,p3").split(",")   # имена хостов заглушек
DB_URL = os.getenv("DB_URL", "postgresql+psycopg://postgres:postgres@db:5432/postgres")
BUDGET = 2.0        # с на поставщика, включая повтор
RETRY_PAUSE = 0.2   # с, пауза перед повтором

flights = Table("flights", MetaData(),
                Column("carrier", String), Column("flight_no", String),
                Column("origin", String), Column("destination", String),
                Column("dep_time", DateTime), Column("arr_time", DateTime),
                Column("price", Integer))


def day_query(day: date):
    start = datetime.combine(day, dtime.min)
    return select(flights).where(flights.c.dep_time >= start,
                                 flights.c.dep_time < start + timedelta(days=1))


def parse(items: list[dict]) -> list[Flight]:
    return [Flight(i["carrier"], i["flight_no"], i["origin"], i["destination"],
                   datetime.fromisoformat(i["dep_time"]),
                   datetime.fromisoformat(i["arr_time"]), i["price"]) for i in items]


def answer(own: list[Flight], got: dict, origin: str, destination: str, t0: float):
    """got: {поставщик: список рейсов в JSON или None, если не ответил}. Только вычисления."""
    failed = [p for p, v in got.items() if v is None]
    if len(failed) == len(got) and not own:
        raise HTTPException(503, "Поиск временно недоступен")
    allf = own + [f for v in got.values() if v for f in parse(v)]
    routes = build_routes(allf, origin, destination)
    return {"routes": [{"price": s, "flights": [f._asdict() for f in p]} for s, p in routes],
            "failed_providers": failed,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000)}
