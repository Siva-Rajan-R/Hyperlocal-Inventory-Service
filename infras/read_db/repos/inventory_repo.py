from ..main import INVENTORY_COLLECTION
from ..models.prod_inv_model import ProdInvReadModel
from icecream import ic
from schemas.v1.request_schemas.inventory_schema import GetAllInventorySchema, GetInventoryByShopIdSchema, GetInventoryByIdSchema
from typing import List, Dict

class InventoryReadDbRepo:

    @staticmethod
    async def create_inventory(inventory: ProdInvReadModel):
        try:
            document = inventory.model_dump(mode="json")
            result = await INVENTORY_COLLECTION.insert_one(document)
            ic(f"Read DB inventory created: {result.inserted_id}")
            return True
        except Exception as e:
            ic(f"Error saving inventory to Read DB: {e}")
            return False

    @staticmethod
    async def replace_inventory(inventory: ProdInvReadModel):
        try:
            document = inventory.model_dump(mode="json")
            result = await INVENTORY_COLLECTION.replace_one(
                {"id": inventory.id, "shop_id": inventory.shop_id},
                document,
                upsert=True
            )
            ic(f"Read DB inventory replaced/updated: {result.modified_count}")
            return True
        except Exception as e:
            ic(f"Error replacing inventory in Read DB: {e}")
            return False

    @staticmethod
    async def get_all_inventories(data: GetAllInventorySchema | GetInventoryByShopIdSchema):
        query = {}
        if hasattr(data, 'shop_id') and data.shop_id:
            query["shop_id"] = data.shop_id
            
        if getattr(data, 'is_active', None) is not None:
            query["is_active"] = data.is_active

        if getattr(data, 'from_date', None) or getattr(data, 'to_date', None):
            created_at_query = {}
            if getattr(data, 'from_date', None):
                created_at_query["$gte"] = data.from_date
            if getattr(data, 'to_date', None):
                to_date_str = data.to_date
                if len(to_date_str) <= 10:
                    to_date_str += "T23:59:59"
                created_at_query["$lte"] = to_date_str
            if created_at_query:
                query["created_at"] = created_at_query

        if getattr(data, 'query', None):
            query["$or"] = [
                {"id": {"$regex": data.query, "$options": "i"}},
                {"name": {"$regex": data.query, "$options": "i"}},
                {"description": {"$regex": data.query, "$options": "i"}},
                {"category": {"$regex": data.query, "$options": "i"}},
                {"barcode": {"$regex": data.query, "$options": "i"}},
                {"sku": {"$regex": data.query, "$options": "i"}}
            ]
            
        cursor = data.offset-1 if getattr(data, 'offset', None) and data.offset > 0 else 0
        limit = getattr(data, 'limit', 10)
        inventories_cursor = INVENTORY_COLLECTION.find(query).limit(limit).skip(cursor * limit).sort("created_at", -1)
        docs = await inventories_cursor.to_list(length=limit)
        
        for doc in docs:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
                
        return docs

    @staticmethod
    async def get_inventory_by_id(data: GetInventoryByIdSchema):
        query = {"id": data.id, "shop_id": data.shop_id}
        doc = await INVENTORY_COLLECTION.find_one(query)
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    @staticmethod
    async def delete_inventory(inventory_id: str, shop_id: str):
        result = await INVENTORY_COLLECTION.delete_one({"id": inventory_id, "shop_id": shop_id})
        return result.deleted_count > 0
