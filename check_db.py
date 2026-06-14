import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from hyperlocal_platform.core.configs.settings_config import SETTINGS

async def check():
    engine = create_async_engine(SETTINGS.DATABASE_URL)
    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT id, name, datas FROM inventory WHERE name = 'hello image test'"))
        row = res.fetchone()
        print("Postgres:", dict(row) if row else None)

asyncio.run(check())
