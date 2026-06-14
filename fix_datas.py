import asyncio
from sqlalchemy import text
from infras.primary_db.main import AsyncInventoryLocalSession

async def fix_datas():
    async with AsyncInventoryLocalSession() as session:
        await session.execute(text("UPDATE inventory SET datas = datas->0 WHERE id = '689e78eb-e328-5145-b282-cb3c1c168475' AND jsonb_typeof(datas) = 'array'"))
        await session.commit()
        print("Fixed datas array")

if __name__ == "__main__":
    asyncio.run(fix_datas())
