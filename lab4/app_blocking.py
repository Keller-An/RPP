"""Версия 2 (задача 4.1): те же вызовы, но обработчики объявлены через async def."""
from datetime import date as Date

from fastapi import FastAPI

import app_sync

app = FastAPI()


@app.get("/search")
async def search(origin: str, destination: str, date: Date):
    return app_sync.search(origin, destination, date)    # блокирует цикл событий


@app.get("/health")
async def health():
    return {"status": "ok"}
