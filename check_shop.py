import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
async def run():
    engine = create_async_engine('postgresql+asyncpg://postgres:437734@localhost:5432/InventoryServiceDb')
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT shop_id FROM products WHERE id='73879304-4d4f-5fd4-8887-a108d112c3b0'"))
        print(res.fetchall())
asyncio.run(run())
