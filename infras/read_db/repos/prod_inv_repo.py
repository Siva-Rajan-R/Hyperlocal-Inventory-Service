from pydoc import doc
from typing import List,Optional
from annotated_types import doc
from sqlalchemy.ext.asyncio import AsyncSession
from pymongo import UpdateOne
from icecream import ic
from ..main import PROD_INV_COLLECTION
from schemas.v1.product_schemas.request_schemas import GetBulkProductsById,GetAllProductSchema,GetProductsByShopId,GetProductsById
from integrations.utility_service import get_shop_category, get_shop_unit
from infras.primary_db.repos.product_repo import ProductRepo
from infras.primary_db.main import AsyncInventoryLocalSession

class ProdInvReadDbRepo:

    @classmethod
    async def add_updatereaddb(cls, shop_id: str, product_ids: List[str], session: AsyncSession):
        try:
            # Fetch products from Primary DB
            primary_repo = ProductRepo(session=session)
            request_data = GetBulkProductsById(
                shop_id=shop_id,
                include_serialno=True,
                id=product_ids
            )
            primary_products = await primary_repo.get_bulk_products_by_id(data=request_data)
            ic(primary_products)
            if not primary_products:
                return False

            # Fetch existing read models to compare and reuse category/unit names
            existing_cursor = PROD_INV_COLLECTION.find(
                {"id": {"$in": product_ids}, "shop_id": shop_id},
                {"id": 1, "category_infos": 1, "unit_infos": 1}
            )
            existing_products = await existing_cursor.to_list(length=len(product_ids))
            existing_map = {p["id"]: p for p in existing_products}

            bulk_ops = []

            for p_data in primary_products:
                prod_id = p_data["id"]
                category_id = p_data.get("category_id")
                unit_id = p_data.get("unit_id")
                
                existing_prod = existing_map.get(prod_id)
                
                category_name = ""
                unit_name = ""

                # Reuse existing names if ID matches
                if existing_prod:
                    ext_cat = existing_prod.get("category_infos", {})
                    ext_unit = existing_prod.get("unit_infos", {})
                    
                    if ext_cat and ext_cat.get("id") == category_id:
                        category_name = ext_cat.get("name", "")
                    
                    if ext_unit and ext_unit.get("id") == unit_id:
                        unit_name = ext_unit.get("name", "")

                # Fetch from Utility Service if not matched/found
                if not category_name and category_id:
                    cat_res = await get_shop_category(shop_id=shop_id, category_id=category_id)
                    if isinstance(cat_res, dict):
                        category_name = cat_res.get("name", "")
                    
                if not unit_name and unit_id:
                    unit_res = await get_shop_unit(shop_id=shop_id, unit_id=unit_id)
                    if isinstance(unit_res, dict):
                        unit_name = unit_res.get("name", "")

                # Add infos to the primary DB structure
                p_data["category_infos"] = {
                    "id": category_id,
                    "name": category_name
                }
                p_data["unit_infos"] = {
                    "id": unit_id,
                    "name": unit_name
                }
                
                # Fetch Custom Fields
                from infras.primary_db.services.customfield_service import CustomFieldsService
                cf_service = CustomFieldsService(session=session)
                try:
                    cf_values = await cf_service.get_values_by_product(product_id=prod_id, shop_id=shop_id)
                    p_data["custom_fields"] = {v["field_name"]: v["value"] for v in cf_values} if cf_values else {}
                except Exception as e:
                    ic(f"Error fetching custom fields for read db: {e}")
                    p_data["custom_fields"] = {}
                
                # Prepare bulk operation
                bulk_ops.append(
                    UpdateOne(
                        {"id": prod_id, "shop_id": shop_id},
                        {"$set": p_data},
                        upsert=True
                    )
                )

            # Execute bulk write
            if bulk_ops:
                res = await PROD_INV_COLLECTION.bulk_write(bulk_ops)
                ic(f"Read DB Bulk Write Result: inserted={res.inserted_count}, modified={res.modified_count}, upserted={res.upserted_count}")
                return True

            return False

        except Exception as e:
            ic(f"Error in add_updatereaddb: {e}")
            return False

    

    @classmethod
    async def get_all(
        cls,
        data:GetAllProductSchema
    ) -> List[dict]:
        try:
            query = {}

            if data.active is not None:
                query["is_active"] = data.active

            cursor = PROD_INV_COLLECTION.find(query)
            
            data = await cursor.to_list(length=None)
            for d in data:
                d["_id"] = str(d["_id"])
            
            return data

        except Exception as e:
            ic(f"Error in get_all: {e}")
            return []

    @classmethod
    async def get_by_shop_id(
        cls,
        data:GetProductsByShopId
    ) -> List[dict]:
        try:
            query = {
                "shop_id": data.shop_id
            }

            if data.active is not None:
                query["is_active"] = data.active

            cursor = PROD_INV_COLLECTION.find(query)

            data = await cursor.to_list(length=None)
            for d in data:
                d["_id"] = str(d["_id"])
            
            return data

        except Exception as e:
            ic(f"Error in get_by_shop_id: {e}")
            return []

    @classmethod
    async def get_by_id(
        cls,
        data:GetProductsById,
    ) -> Optional[dict]:
        try:
            query = {
                "shop_id": data.shop_id,
                "id": data.id
            }

            if data.active is not None:
                query["is_active"] = data.active

            data = await PROD_INV_COLLECTION.find_one(query)
            if data:
                data["_id"] = str(data["_id"])

            return data

        except Exception as e:
            ic(f"Error in get_by_id: {e}")
            return None


    @classmethod   
    async def get_bulk_by_id(
        cls,
        data:GetBulkProductsById,
    ) -> List[dict]:
        try:
            query = {
                "shop_id": data.shop_id,
                "id": {"$in": data.id},
            }

            if data.active is not None:
                query["is_active"] = data.active

            cursor = PROD_INV_COLLECTION.find(
                query,
                {"_id": 0},  # Exclude MongoDB ObjectId
            )

            return await cursor.to_list(length=len(data.id))

        except Exception as e:
            ic(f"Error in get_bulk_by_id: {e}")
            return []
        


