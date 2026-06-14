from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["InventoryServiceDb"]

# Find the test variant product
doc = db['inventory_collections'].find_one({"name": {"$regex": "VARIANT", "$options": "i"}})
print("Doc by name:", doc)

# Find any product with variants
doc2 = db['inventory_collections'].find_one({"has_variant": True})
print("Doc by has_variant:", doc2)
