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
                    
                sub_units = []
                if existing_prod:
                    ext_unit = existing_prod.get("unit_infos", {})
                    if ext_unit and ext_unit.get("id") == unit_id:
                        unit_name = ext_unit.get("name", "")
                        sub_units = ext_unit.get("sub_units") or []

                # Fetch from Utility Service if not matched/found
                if not category_name and category_id:
                    cat_res = await get_shop_category(shop_id=shop_id, category_id=category_id)
                    if isinstance(cat_res, dict):
                        category_name = cat_res.get("name", "")
                    
                if (not unit_name or not sub_units) and unit_id:
                    unit_res = await get_shop_unit(shop_id=shop_id, unit_id=unit_id)
                    if isinstance(unit_res, dict):
                        unit_name = unit_res.get("name", "")
                        sub_units = unit_res.get("sub_units") or []

                # Add infos to the primary DB structure
                p_data["category_infos"] = {
                    "id": category_id,
                    "name": category_name
                }
                p_data["unit_infos"] = {
                    "id": unit_id,
                    "name": unit_name,
                    "sub_units": sub_units
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
    def _build_search_query(cls, data, base_query: dict = None) -> dict:
        from datetime import datetime
        query = dict(base_query) if base_query else {}

        if data.active is not None:
            query["is_active"] = data.active

        if data.visible_online is not None:
            query["visible_online"] = data.visible_online

        if getattr(data, 'have_tracking', None) is not None:
            query["have_tracking"] = data.have_tracking

        search_q = getattr(data, 'query', None) or getattr(data, 'q', None)
        if search_q:
            q_str = str(search_q).strip()
            if q_str:
                import re
                escaped_q = re.escape(q_str)
                regex = {"$regex": escaped_q, "$options": "i"}
                query["$or"] = [
                    {"name": regex},
                    {"id": regex},
                    {"ui_id": regex},
                    {"sku": regex},
                    {"barcode": regex},
                    {"brand": regex},
                    {"category_id": regex},
                    {"category_infos.name": regex},
                    {"category_infos.id": regex},
                    {"unit_id": regex},
                    {"unit_infos.name": regex},
                    {"unit_infos.id": regex},
                    {"variants.name": regex},
                    {"variants.sku": regex},
                    {"variants.barcode": regex},
                    {"variants.ui_id": regex},
                    {"variants.id": regex},
                    {
                        "$expr": {
                            "$cond": {
                                "if": { "$eq": [{ "$type": "$variants" }, "object"] },
                                "then": {
                                    "$gt": [
                                        {
                                            "$size": {
                                                "$filter": {
                                                    "input": { "$objectToArray": "$variants" },
                                                    "as": "v",
                                                    "cond": {
                                                        "$or": [
                                                            { "$regexMatch": { "input": { "$ifNull": ["$$v.v.name", ""] }, "regex": escaped_q, "options": "i" } },
                                                            { "$regexMatch": { "input": { "$ifNull": ["$$v.v.sku", ""] }, "regex": escaped_q, "options": "i" } },
                                                            { "$regexMatch": { "input": { "$ifNull": ["$$v.v.barcode", ""] }, "regex": escaped_q, "options": "i" } },
                                                            { "$regexMatch": { "input": { "$ifNull": ["$$v.v.ui_id", ""] }, "regex": escaped_q, "options": "i" } },
                                                            { "$regexMatch": { "input": { "$ifNull": ["$$v.v.id", ""] }, "regex": escaped_q, "options": "i" } }
                                                        ]
                                                    }
                                                }
                                            }
                                        },
                                        0
                                    ]
                                },
                                "else": False
                            }
                        }
                    }
                ]

        if getattr(data, 'from_date', None):
            try:
                from_dt = datetime.strptime(data.from_date, "%Y-%m-%d")
                if "created_at" not in query:
                    query["created_at"] = {}
                query["created_at"]["$gte"] = from_dt
            except Exception:
                pass

        if getattr(data, 'to_date', None):
            try:
                to_date_str = data.to_date
                if len(to_date_str) <= 10:
                    to_date_str += ' 23:59:59'
                to_dt = datetime.strptime(to_date_str, "%Y-%m-%d %H:%M:%S")
                if "created_at" not in query:
                    query["created_at"] = {}
                query["created_at"]["$lte"] = to_dt
            except Exception:
                pass

        if getattr(data, 'stock_status', None):
            status_val = data.stock_status.lower().strip()
            if status_val in ["no", "no_stock", "out_of_stock"]:
                query["stock_infos.available_stocks"] = {"$lte": 0}
            elif status_val in ["low", "low_stock"]:
                query["$expr"] = {"$lte": ["$stock_infos.available_stocks", "$reorder_point_infos.reorder_point"]}

        return query

    @classmethod
    async def get_all(
        cls,
        data:GetAllProductSchema
    ) -> List[dict]:
        try:
            query = cls._build_search_query(data)
            cursor = PROD_INV_COLLECTION.find(query)
            if getattr(data, 'limit', None):
                offset = data.offset - 1 if (data.offset and data.offset > 0) else 0
                cursor = cursor.skip(offset * data.limit).limit(data.limit)
            
            data_res = await cursor.to_list(length=None)
            for d in data_res:
                d["_id"] = str(d["_id"])
            
            return data_res

        except Exception as e:
            ic(f"Error in get_all: {e}")
            return []

    @classmethod
    async def get_by_shop_id(
        cls,
        data:GetProductsByShopId
    ) -> List[dict]:
        try:
            query = cls._build_search_query(data, base_query={"shop_id": data.shop_id})
            cursor = PROD_INV_COLLECTION.find(query)
            if getattr(data, 'limit', None):
                offset = data.offset - 1 if (data.offset and data.offset > 0) else 0
                cursor = cursor.skip(offset * data.limit).limit(data.limit)

            data_res = await cursor.to_list(length=None)
            for d in data_res:
                d["_id"] = str(d["_id"])
            
            return data_res

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

            if data.visible_online is not None:
                query["visible_online"] = data.visible_online

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
                "id": {"$in": data.id},
            }
            if data.shop_id:
                query["shop_id"] = data.shop_id

            if data.active is not None:
                query["is_active"] = data.active

            if data.visible_online is not None:
                query["visible_online"] = data.visible_online

            cursor = PROD_INV_COLLECTION.find(
                query,
                {"_id": 0},  # Exclude MongoDB ObjectId
            )

            return await cursor.to_list(length=len(data.id))

        except Exception as e:
            ic(f"Error in get_bulk_by_id: {e}")
            return []

    @classmethod
    async def delete_product(cls, product_id: str, shop_id: str) -> bool:
        try:
            result = await PROD_INV_COLLECTION.delete_one({"id": product_id, "shop_id": shop_id})
            return result.deleted_count > 0
        except Exception as e:
            ic(f"Error deleting product from read DB: {e}")
            return False

        


