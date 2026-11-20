"""Общие данные: аэропорты, генератор рейсов, составление маршрутов (без сети и БД)."""
import heapq
import random
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import NamedTuple

AIRPORTS = ("SVO DME LED OVB KJA SVX KZN AER KRR ROV UFA KGD MRV GOJ CEK OMS "
            "TJM IKT VVO KHV PEE VOG KUF MMK ARH YKS UUS MCX NJC HTA").split()
START = date(2026, 11, 1)   # первая дата расписания
DAYS = 120                  # число дней расписания


class Flight(NamedTuple):
    carrier: str
    flight_no: str
    origin: str
    destination: str
    dep_time: datetime
    arr_time: datetime
    price: int


def gen_flights(carrier: str, day: date, n: int) -> list[Flight]:
    """n рейсов перевозчика на дату; для одних и тех же аргументов результат одинаков."""
    rng = random.Random(f"{carrier}-{day}")
    midnight = datetime.combine(day, time.min)
    res = []
    for i in range(n):
        origin, dest = rng.sample(AIRPORTS, 2)
        dep = midnight + timedelta(minutes=rng.randrange(1440))
        arr = dep + timedelta(minutes=rng.randrange(60, 361))   # полёт 1-6 часов
        res.append(Flight(carrier, f"{carrier}{i:04d}", origin, dest, dep, arr,
                          rng.randrange(30, 301) * 100))
    return res


def build_routes(flights: list[Flight], origin: str, dest: str, limit: int = 20):
    """Маршруты (прямые и до 2 пересадок), пересадка 1-6 ч, аэропорты не повторяются.
    Возвращает до limit самых дешёвых: [(цена, [рейсы])]."""
    if origin == dest:
        return []
    by_origin = defaultdict(list)
    for f in sorted(flights, key=lambda f: f.dep_time):
        by_origin[f.origin].append(f)
    deps = {a: [f.dep_time for f in fl] for a, fl in by_origin.items()}
    found = []

    def walk(airport, lo, hi, path, price, seen):
        fl = by_origin.get(airport, ())
        i = bisect_left(deps[airport], lo) if lo and fl else 0
        j = bisect_right(deps[airport], hi) if hi and fl else len(fl)
        for f in fl[i:j]:
            if f.destination in seen:
                continue
            p, s = path + [f], price + f.price
            if f.destination == dest:
                found.append((s, p))
            elif len(p) < 3:
                walk(f.destination, f.arr_time + timedelta(hours=1),
                     f.arr_time + timedelta(hours=6), p, s, seen | {f.destination})

    walk(origin, None, None, [], 0, {origin})
    return heapq.nsmallest(limit, found, key=lambda r: r[0])
