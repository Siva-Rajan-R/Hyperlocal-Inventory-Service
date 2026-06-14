import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def check():
    engine = create_async_engine("postgresql+asyncpg://postgres:TempSuperSecretPwd@89.167.72.254:5432/InventoryServiceDb")
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT id, name, datas FROM inventory WHERE name = 'hello image test'"))
        row = res.fetchone()
        print("Postgres datas for 'hello image test':", dict(row._mapping) if row else None)

asyncio.run(check())
