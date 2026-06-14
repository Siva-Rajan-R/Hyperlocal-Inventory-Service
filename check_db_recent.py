import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def check():
    engine = create_async_engine("postgresql+asyncpg://postgres:TempSuperSecretPwd@89.167.72.254:5432/InventoryServiceDb")
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT id, name, created_at, updated_at, datas FROM inventory ORDER BY updated_at DESC LIMIT 5"))
        rows = res.fetchall()
        for r in rows:
            print(r._mapping['name'], "| created:", r._mapping['created_at'], "| updated:", r._mapping['updated_at'], "| images:", r._mapping['datas'].get('images', []))

asyncio.run(check())
