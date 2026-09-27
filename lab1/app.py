import os
from datetime import datetime

from flask import Flask, request
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

# Строка подключения собирается из переменных окружения, а не зашита в код
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Visit(Base):
    __tablename__ = "visits"

    id = Column(Integer, primary_key=True)
    visited_at = Column(DateTime, default=datetime.utcnow)
    ip_address = Column(String(45))


# Таблица создаётся при старте приложения
Base.metadata.create_all(bind=engine)

app = Flask(__name__)


@app.route("/hello", methods=["GET"])
def hello():
    visited_at = datetime.utcnow()
    ip_address = request.remote_addr

    session = SessionLocal()
    try:
        visit = Visit(visited_at=visited_at, ip_address=ip_address)
        session.add(visit)
        session.commit()
    finally:
        session.close()

    return "Hello", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)