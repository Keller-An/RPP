import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://loyalty:loyalty_pass@postgres:5432/loyalty"
)

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)


async def check_database():
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def get_operation(operation_id: str):
    query = text("""
        SELECT operation_id, type, customer_id, store_id, check_number,
               amount, points, points_delta, balance_after, status,
               reject_reason, occurred_at, processed_at
        FROM operations
        WHERE operation_id = :operation_id
    """)
    async with engine.connect() as connection:
        result = await connection.execute(
            query, {"operation_id": operation_id}
        )
        return result.mappings().first()


async def get_account(customer_id: str):
    account_query = text("""
        SELECT customer_id, balance
        FROM accounts
        WHERE customer_id = :customer_id
    """)

    operations_query = text("""
        SELECT operation_id, type, store_id, check_number,
               amount, points, points_delta, balance_after,
               status, reject_reason, occurred_at, processed_at
        FROM operations
        WHERE customer_id = :customer_id
        ORDER BY processed_at DESC
        LIMIT 20
    """)

    async with engine.connect() as connection:
        account = (
            await connection.execute(
                account_query, {"customer_id": customer_id}
            )
        ).mappings().first()

        if account is None:
            return None

        operations = (
            await connection.execute(
                operations_query, {"customer_id": customer_id}
            )
        ).mappings().all()

        return account, operations
