import asyncio
from infras.primary_db.main import get_pg_async_session
from sqlalchemy import select
from infras.primary_db.models.inventory_model import InventoryModel

async def run():
    session_gen = get_pg_async_session()
    session = await anext(session_gen)
    result = await session.execute(select(InventoryModel).where(InventoryModel.name.ilike('%VARIANT%')))
    docs = result.scalars().all()
    for d in docs:
        print(d.__dict__)

asyncio.run(run())
