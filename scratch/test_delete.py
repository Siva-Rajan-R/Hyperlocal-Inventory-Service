import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infras.primary_db.main import AsyncInventoryLocalSession
from infras.primary_db.repos.product_repo import ProductRepo
from icecream import ic
from sqlalchemy import text

async def test_delete():
    async with AsyncInventoryLocalSession() as db:
        repo = ProductRepo(session=db)
        
        serialno_todelete = ['f4691ac0-c296-594e-9df3-cc2867d2ae80', 'yuiyuuigyui']
        ic("Deleting:", serialno_todelete)
        
        res = await repo.delete_bulk_serialno(data=serialno_todelete)
        ic("Result:", res)
        
        # also print if they are still there
        stmt = text("SELECT id, name FROM product_serialnumbers WHERE name = 'yuiyuuigyui'")
        remaining = (await db.execute(stmt)).mappings().all()
        ic("Remaining in DB:", remaining)

if __name__ == "__main__":
    asyncio.run(test_delete())
