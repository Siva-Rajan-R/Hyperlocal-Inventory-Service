import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schemas.v1.prod_inv_schemas.request_schemas import UpdateAllProdInvSchema
from icecream import ic

def test_schema():
    data = [
        {
            "product_id": "2cc177cc-e149-594c-872e-5dfc7caf221e",
            "shop_id": "3f74a412-68d8-5e16-864e-e2f0bc488150",
            "stocks": 1.0,
            "type": "DECREMENT",
            "entity_name": "OFFLINE_PURCHASE_RETURN",
            "serialno_infos": [
                {
                    "id": "36c55ad1-f7d3-572e-9322-a1e1a3ed4c69"
                }
            ]
        }
    ]

    parsed = [UpdateAllProdInvSchema(**d) for d in data]
    ic("Parsed Schema =>", parsed)
    assert parsed[0].serialno_infos[0].id == "36c55ad1-f7d3-572e-9322-a1e1a3ed4c69"
    assert parsed[0].serialno_infos[0].name is None
    print("SCHEMA PARSING TEST PASSED CLEANLY!")

if __name__ == "__main__":
    test_schema()
