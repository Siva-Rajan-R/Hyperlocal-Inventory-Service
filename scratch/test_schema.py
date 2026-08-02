import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schemas.v1.prod_inv_schemas.request_schemas import UpdateAllProdInvSchema
from icecream import ic

def test_schema():
    payload = {
        "shop_id": "shop_1",
        "product_id": "prod_1",
        "type": "DECREMENT",
        "stocks": 1.0,
        "entity_name": "OFFLINE_PURCHASE_RETURN",
        "serialno_infos": [{"id": "id1", "name": "name1"}]
    }
    parsed = UpdateAllProdInvSchema(**payload)
    ic(parsed)
    
    d_dict = parsed.dict(exclude_unset=True)
    ic(d_dict)

if __name__ == "__main__":
    test_schema()
