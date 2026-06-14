import asyncio
from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
from infras.read_db.models.inventory_model import InventoryReadModel

async def test():
    read_model = InventoryReadModel(
        id="test-id",
        shop_id="TEST-SHOP",
        name="test read db",
        category="Test",
        stocks=0,
        reorder_point=0,
        buy_price=0,
        sell_price=0,
        sku="TEST-SKU",
        has_variant=False,
        has_batch=False,
        has_serialno=False,
        is_active=True,
        datas={"images": ["http://test.image/url"]}
    )
    
    document = read_model.model_dump(mode="json")
    print("Document:", document)

asyncio.run(test())
