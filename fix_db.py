import asyncio
from infras.primary_db.main import AsyncInventoryLocalSession
from sqlalchemy import text

async def fix():
    async with AsyncInventoryLocalSession() as session:
        await session.execute(text("UPDATE inventory_stocks SET physical_stocks = 8, available_stocks = 8, reserved_stocks = 0 WHERE product_id = '73879304-4d4f-5fd4-8887-a108d112c3b0'"))
        await session.commit()
    print("DB fixed")

asyncio.run(fix())
