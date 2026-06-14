import asyncio
from schemas.v1.request_schemas.inventory_schema import UpdateInventorySchema

payload = {
    "id": "abc",
    "shop_id": "TEST",
    "name": "test",
    "datas": {
        "images": ["http://test"]
    }
}

schema = UpdateInventorySchema(**payload)
print("Schema dict:", schema.model_dump())
print("Exclude unset:", schema.model_dump(exclude_unset=True, exclude_none=True))
