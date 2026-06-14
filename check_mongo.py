import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

async def check():
    client = AsyncIOMotorClient("mongodb://localhost:27017/")
    db = client["InventoryServiceDb"]
    collection = db["inventory_collections"]
    
    docs = await collection.find({}).sort("updated_at", -1).to_list(5)
    for doc in docs:
        print(doc.get("name"), "| updated:", doc.get("updated_at"), "| images:", doc.get("datas", {}).get("images", []))

asyncio.run(check())
