import asyncio
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from infras.primary_db.main import AsyncInventoryLocalSession
from infras.primary_db.models.product_model import ProductSerialNumbers

async def fetch_serials():
    async with AsyncInventoryLocalSession() as db:
        from sqlalchemy import text
        stmt = text("SELECT id, name, status FROM product_serialnumbers WHERE id = '5cef486c-5846-5940-882a-0b67e2527a4d'")
        res = (await db.execute(stmt)).mappings().all()
        
        output = []
        for r in res:
            output.append({
                "id": r["id"],
                "name": r["name"],
                "status": r["status"]
            })
            
        with open("scratch/serial_dump.json", "w") as f:
            json.dump(output, f, indent=4)
        print("Dumped.")

if __name__ == "__main__":
    asyncio.run(fetch_serials())
