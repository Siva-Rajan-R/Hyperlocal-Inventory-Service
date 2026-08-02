import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schemas.v1.prod_inv_schemas.request_schemas import UpdateAllProdInvSchema
from messaging.msgqueue_services.prod_inv_msgqueue_service import MessagingQueueProductInvService
from icecream import ic

async def test_update():
    payload = {
        "shop_id": "3f74a412-68d8-5e16-864e-e2f0bc488150",
        "product_id": "2cc177cc-e149-594c-872e-5dfc7caf221e",
        "type": "DECREMENT",
        "stocks": 1.0,
        "entity_name": "OFFLINE_PURCHASE_RETURN",
        "serialno_infos": [{"id": "5cef486c-5846-5940-882a-0b67e2527a4d", "name": "EFERFER"}]
    }
    
    svc = MessagingQueueProductInvService()
    res = await svc.update_bulk_prodinv(data=[payload])
    ic(res)

if __name__ == "__main__":
    asyncio.run(test_update())
