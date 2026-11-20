"""Генератор рейсов собственной базы (1500 в сутки, 120 дней) и загрузка в PostgreSQL."""
import os
from datetime import timedelta

import psycopg

from common import DAYS, START, gen_flights

with psycopg.connect(os.environ["DB_URL"].replace("+psycopg", "")) as conn:
    cur = conn.cursor()
    cur.execute("TRUNCATE flights")          # повторный запуск даёт ту же таблицу
    with cur.copy("COPY flights (carrier, flight_no, origin, destination, "
                  "dep_time, arr_time, price) FROM STDIN") as cp:
        for d in range(DAYS):
            for f in gen_flights("OWN", START + timedelta(days=d), 1500):
                cp.write_row(f)
    cur.execute("ANALYZE flights")
    print("рейсов в базе:", cur.execute("SELECT count(*) FROM flights").fetchone()[0])
