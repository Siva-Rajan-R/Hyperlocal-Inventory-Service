import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, delete, or_
from infras.primary_db.main import get_db
from models.product_models import ProductSerialNumbers
from icecream import ic

async def test_uuid_issue():
    async for db in get_db():
        clean_data = ["36c55ad1-f7d3-572e-9322-a1e1a3ed4c69", "Serial-A-123"]
        stmt = (
            delete(ProductSerialNumbers)
            .where(
                or_(
                    ProductSerialNumbers.id.in_(clean_data),
                    ProductSerialNumbers.name.in_(clean_data)
                )
            )
            .returning(ProductSerialNumbers.id)
        )
        try:
            res = (await db.execute(stmt)).scalars().all()
            ic(res)
        except Exception as e:
            ic("Error!", str(e))
        break

if __name__ == "__main__":
    asyncio.run(test_uuid_issue())
