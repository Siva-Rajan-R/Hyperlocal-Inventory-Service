import asyncio
from infras.primary_db.main import AsyncInventoryLocalSession
from infras.primary_db.repos.inventory_repo import InventoryRepo
from schemas.v1.request_schemas.inventory_schema import GetInventoryByIdSchema
from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
from infras.read_db.models.inventory_model import InventoryReadModel

async def sync_dbs():
    inv_ids = [
        '3dbfed38-f74b-52a5-a0ae-476b981fa897',
        '5a90591c-f052-518a-bd38-2049a20051c8',
        '05ddb8c6-3073-5124-ad02-a390a510b83f'
    ]
    shop_id = 'TEST-SHOP'
    
    async with AsyncInventoryLocalSession() as session:
        inv_repo_obj = InventoryRepo(session=session)
        for inv_id in inv_ids:
            raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=shop_id, id=inv_id))
            if raw_inventory:
                await InventoryReadDbRepo.replace_inventory(InventoryReadModel(**raw_inventory))
                print(f"Synced {inv_id}")

if __name__ == "__main__":
    asyncio.run(sync_dbs())
