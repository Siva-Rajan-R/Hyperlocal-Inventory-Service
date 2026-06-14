import asyncio
import httpx

async def test_update():
    payload = {
        "id": "c1dbbf20-023e-593d-a174-74d7a33d66ff", # frerrgeg hello samepls
        "shop_id": "TEST-SHOP",
        "name": "frerrgeg hello samepls",
        "category": "Clothing",
        "description": "test",
        "has_serialno": False,
        "has_batch": False,
        "datas": {
            "brand": "frerrgeg",
            "unit": "Piece (pcs)",
            "mrp": 0,
            "gst": "18%",
            "hsn": "",
            "supplier": "",
            "opening_stock": 0,
            "storage_location": "",
            "is_active": False,
            "variant_types": [],
            "images": ["http://test.image/url/new"]
        }
    }
    
    async with httpx.AsyncClient() as client:
        res = await client.put("http://localhost:8005/inventories/inventories", json=payload)
        print("Status Code:", res.status_code)
        print("Response:", res.text)

asyncio.run(test_update())
