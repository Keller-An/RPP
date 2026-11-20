"""Нагрузочный скрипт. Замкнутый режим: python load.py closed -c 10
Открытый режим: python load.py open -r 5"""
import argparse
import asyncio
import itertools
import math
import random
import time
from datetime import timedelta

import httpx

from common import AIRPORTS, DAYS, START

ap = argparse.ArgumentParser()
ap.add_argument("mode", choices=["closed", "open"])
ap.add_argument("-c", "--clients", type=int, default=1)       # замкнутый: число клиентов
ap.add_argument("-r", "--rate", type=float, default=5)        # открытый: запросов в секунду
ap.add_argument("-d", "--duration", type=float, default=60)   # длительность замера, с
ap.add_argument("-w", "--warmup", type=float, default=10)     # прогрев, с
ap.add_argument("-t", "--timeout", type=float, default=30)    # тайм-аут клиента, с
ap.add_argument("-u", "--url", default="http://localhost:8000")
args = ap.parse_args()

rng = random.Random(42)                                       # одинаковый набор запросов
QUERIES = itertools.cycle([
    {"origin": o, "destination": d, "date": str(START + timedelta(days=rng.randrange(DAYS)))}
    for o, d in (rng.sample(AIRPORTS, 2) for _ in range(500))])


class Stats:
    def __init__(self):
        self.lat, self.err = [], 0

    def add(self, ms, ok):
        self.lat.append(ms)
        self.err += not ok


async def call(client, path, params, stats, t_from, t_to):
    t = time.perf_counter()
    try:
        ok = (await client.get(path, params=params)).status_code < 400
    except httpx.HTTPError:                                   # в т.ч. тайм-аут клиента
        ok = False
    if t_from <= t < t_to:                                    # прогрев не учитываем
        stats.add((time.perf_counter() - t) * 1000, ok)


async def closed_client(client, stats, t_from, t_to):
    while time.perf_counter() < t_to:
        await call(client, "/search", next(QUERIES), stats, t_from, t_to)


async def open_flow(client, stats, t_from, t_to):
    tasks, nxt = [], time.perf_counter()
    while nxt < t_to:
        tasks.append(asyncio.create_task(call(client, "/search", next(QUERIES), stats, t_from, t_to)))
        nxt += rng.expovariate(args.rate)                     # экспоненциальные интервалы
        await asyncio.sleep(max(0, nxt - time.perf_counter()))
    await asyncio.gather(*tasks)


async def health_poll(client, stats, t_from, t_to):
    nxt = time.perf_counter()
    while time.perf_counter() < t_to:
        await call(client, "/health", None, stats, t_from, t_to)
        nxt += 0.1
        await asyncio.sleep(max(0, nxt - time.perf_counter()))


def pct(v, p):                                                # метод ближайшего ранга
    return v[max(0, math.ceil(p / 100 * len(v)) - 1)]


def report(name, st):
    v = sorted(st.lat)
    if not v:
        return print(f"{name}: нет данных")
    print(f"{name}: запросов={len(v)}  в секунду={len(v) / args.duration:.2f}  "
          f"p50={pct(v, 50):.0f}  p95={pct(v, 95):.0f}  p99={pct(v, 99):.0f}  "
          f"max={v[-1]:.0f} мс  ошибки={100 * st.err / len(v):.1f} %")


async def main():
    limits, tmo = httpx.Limits(max_connections=None), httpx.Timeout(args.timeout)
    async with httpx.AsyncClient(base_url=args.url, limits=limits, timeout=tmo) as c1, \
               httpx.AsyncClient(base_url=args.url, limits=limits, timeout=tmo) as c2:
        s, h = Stats(), Stats()
        t_from = time.perf_counter() + args.warmup
        t_to = t_from + args.duration
        flows = ([closed_client(c1, s, t_from, t_to) for _ in range(args.clients)]
                 if args.mode == "closed" else [open_flow(c1, s, t_from, t_to)])
        await asyncio.gather(*flows, health_poll(c2, h, t_from, t_to))
    report("/search", s)
    report("/health", h)


asyncio.run(main())
