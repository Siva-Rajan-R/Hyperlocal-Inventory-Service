import asyncio
from schemas.v1.request_schemas.inventory_schema import UpdateInventorySchema
from schemas.v1.db_schemas.inventory_schema import UpdateInventoryDbSchema

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

db_schema = UpdateInventoryDbSchema(**schema.model_dump(mode='json',exclude_none=True,exclude_unset=True))
print("DB Exclude unset:", db_schema.model_dump(mode="json",exclude=['id','shop_id','barcode','offer_offline','offer_online','offer_type'],exclude_unset=True,exclude_none=True))
