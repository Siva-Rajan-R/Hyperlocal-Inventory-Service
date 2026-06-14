import asyncio
import sys
import os

# Add to sys path
sys.path.append(r"d:\projects\airport-marketplace\Services\HyperLocal_Services\Inventory_Service")

from infras.primary_db.main import AsyncInventoryLocalSession
from infras.primary_db.repos.inventory_repo import InventoryRepo
from sqlalchemy import select
from infras.primary_db.models.inventory_model import Inventory

async def test():
    async with AsyncInventoryLocalSession() as session:
        inv_repo = InventoryRepo(session)
        # Fetch an inventory
        res = await session.execute(select(Inventory).limit(1))
        inv = res.scalar_one_or_none()
        if not inv:
            print("No inventory found")
            return
            
        print(f"Before: {inv.stocks}")
        
        # Try to decrease
        data = {inv.id: 2}
        await inv_repo.bulk_qty_decr_update(data=data, shop_id=inv.shop_id)
        
        # Fetch again
        res2 = await session.execute(select(Inventory).where(Inventory.id == inv.id))
        inv2 = res2.scalar_one_or_none()
        print(f"After: {inv2.stocks}")

if __name__ == "__main__":
    asyncio.run(test())
