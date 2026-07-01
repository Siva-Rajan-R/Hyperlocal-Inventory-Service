from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from pymongo import UpdateOne
from icecream import ic
from ..main import PROD_INV_COLLECTION
from schemas.v1.product_schemas.request_schemas import GetBulkProductsById
from integrations.utility_service import get_shop_category, get_shop_unit
from infras.primary_db.repos.product_repo import ProductRepo

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
