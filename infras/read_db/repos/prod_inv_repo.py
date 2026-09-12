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

from core.utils.product_stock_filter import compute_product_stock_and_rop, filter_product_item, has_stock_filter, is_truthy

class ProdInvReadDbRepo:

    compute_product_stock_and_rop = staticmethod(compute_product_stock_and_rop)
    filter_product_item = staticmethod(filter_product_item)
    _has_stock_filter = staticmethod(has_stock_filter)

    @classmethod
    async def add_updatereaddb(cls, shop_id: str, product_ids: List[str], session: AsyncSession):
        try:
            from sqlalchemy import select, text
            from infras.primary_db.models.product_model import ProductSerialNumbers

            # Fetch products from Primary DB (without serialno to avoid ORM relationship cache)
            primary_repo = ProductRepo(session=session)
            request_data = GetBulkProductsById(
                shop_id=shop_id,
                include_serialno=False,  # We fetch serialnos directly below to bypass cache
                id=product_ids
            )
            primary_products = await primary_repo.get_bulk_products_by_id(data=request_data)
            ic(primary_products)
            if not primary_products:
                return False

            # Query serial numbers via raw text SQL using expanding IN clause.
            # Using text() + expanding bindparam bypasses the ORM identity map so that
            # rows deleted earlier in this same transaction are NOT returned.
            # NOTE: ANY(:pids) with asyncpg requires array types — we use IN instead.
            from sqlalchemy import bindparam
            fresh_serialnos: dict = {}
            if product_ids:
                sn_stmt = text(
                    "SELECT id, product_id, variant_id, batch_id, name, status, visible_online "
                    "FROM product_serialnumbers "
                    "WHERE product_id IN :pids AND shop_id = :shop_id"
                ).bindparams(bindparam("pids", expanding=True))
                sn_rows = (await session.execute(
                    sn_stmt,
                    {"pids": list(product_ids), "shop_id": shop_id}
                )).mappings().all()

                ic("Fresh serialno count from DB:", len(sn_rows))
                for row in sn_rows:
                    pid = row["product_id"]
                    if pid not in fresh_serialnos:
                        fresh_serialnos[pid] = []
                    fresh_serialnos[pid].append({
                        "id": row["id"],
                        "variant_id": row["variant_id"],
                        "batch_id": row["batch_id"],
                        "name": row["name"],
                        "status": row["status"],
                        "visible_online": row["visible_online"],
                    })

            ic("fresh_serialnos per product:", {k: len(v) for k, v in fresh_serialnos.items()})

            # Inject the fresh serialno_infos into each product's data structure
            for p_data in primary_products:
                prod_id = p_data.get("id")
                type_infos = p_data.get("type_infos") or {}
                has_serialno = type_infos.get("has_serialno")
                has_variant = type_infos.get("has_variant")
                has_batch = type_infos.get("has_batch")

                product_sn_list = fresh_serialnos.get(prod_id, [])

                if has_serialno:
                    # First, initialize the empty serialno collections so we do not have stale state
                    if has_variant:
                        variants = p_data.get("variants") or {}
                        for vid, v_data in variants.items():
                            if has_batch:
                                batches = v_data.get("batch_infos") or []
                                for b in batches:
                                    b["serialno_infos"] = []
                            else:
                                v_data["serialno_infos"] = []
                    else:
                        if has_batch:
                            batches = p_data.get("batch_infos") or []
                            for b in batches:
                                b["serialno_infos"] = []
                        else:
                            p_data["serialno_infos"] = []

                    # Distribute serial numbers to the correct sub-scope based on variant_id and batch_id
                    for sn in product_sn_list:
                        sn_item = {
                            "id": sn["id"],
                            "name": sn["name"],
                            "status": sn["status"],
                            "visible_online": sn["visible_online"],
                        }
                        v_id = sn["variant_id"]
                        b_id = sn["batch_id"]

                        if has_variant and v_id:
                            variants = p_data.get("variants") or {}
                            variant_data = variants.get(v_id)
                            if variant_data:
                                if has_batch and b_id:
                                    batches = variant_data.get("batch_infos") or []
                                    for b in batches:
                                        if b.get("id") == b_id:
                                            b.setdefault("serialno_infos", []).append(sn_item)
                                            break
                                else:
                                    variant_data.setdefault("serialno_infos", []).append(sn_item)
                        else:
                            if has_batch and b_id:
                                batches = p_data.get("batch_infos") or []
                                for b in batches:
                                    if b.get("id") == b_id:
                                        b.setdefault("serialno_infos", []).append(sn_item)
                                        break
                            else:
                                p_data.setdefault("serialno_infos", []).append(sn_item)
                    ic(f"Injected serials for product {prod_id} successfully.")


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
                existing_cf = p_data.get("custom_fields") or {}
                if not isinstance(existing_cf, dict):
                    existing_cf = {}
                try:
                    cf_values = await cf_service.get_values_by_product(product_id=prod_id, shop_id=shop_id)
                    if cf_values:
                        for v in cf_values:
                            existing_cf[v["field_name"]] = v["value"]
                except Exception as e:
                    ic(f"Error fetching custom fields for read db: {e}")
                p_data["custom_fields"] = existing_cf
                
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
            import traceback
            ic(f"Error in add_updatereaddb: {e}")
            ic(traceback.format_exc())
            return False


    

    @classmethod
    def _build_search_query(cls, data, base_query: dict = None) -> dict:
        from datetime import datetime
        query = dict(base_query) if base_query else {}

        if is_truthy(getattr(data, 'exclude_inactive', None)):
            query["is_active"] = True
        elif is_truthy(getattr(data, 'exclude_active', None)):
            query["is_active"] = False
        elif getattr(data, 'active', None) is not None:
            query["is_active"] = is_truthy(data.active)

        if is_truthy(getattr(data, 'exclude_tracking', None)):
            query["have_tracking"] = False
        elif is_truthy(getattr(data, 'exclude_non_tracking', None)):
            query["have_tracking"] = {"$ne": False}
        elif getattr(data, 'have_tracking', None) is not None:
            query["have_tracking"] = is_truthy(data.have_tracking)

        if getattr(data, 'visible_online', None) is not None:
            query["visible_online"] = is_truthy(data.visible_online)

        if getattr(data, 'category_id', None):
            if "$and" not in query:
                query["$and"] = []
            query["$and"].append({
                "$or": [
                    {"category_id": data.category_id},
                    {"category_infos.id": data.category_id}
                ]
            })

        if getattr(data, 'unit_id', None):
            if "$and" not in query:
                query["$and"] = []
            query["$and"].append({
                "$or": [
                    {"unit_id": data.unit_id},
                    {"unit_infos.id": data.unit_id}
                ]
            })

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

        return query

    @classmethod
    def _has_stock_filter(cls, data) -> bool:
        return any([
            getattr(data, 'exclude_stocks', None) is not None,
            getattr(data, 'exclude_in_stock', None) is not None,
            getattr(data, 'exclude_stock', None) is not None,
            getattr(data, 'exclude_outofstock', None) is not None,
            getattr(data, 'exclude_out_of_stock', None) is not None,
            getattr(data, 'exclude_outofstov', None) is not None,
            getattr(data, 'exclude_no_stock', None) is not None,
            getattr(data, 'exclude_low_stocks', None) is not None,
            getattr(data, 'exclude_low_stock', None) is not None,
            getattr(data, 'exclude_lowstocks', None) is not None,
            getattr(data, 'exclude_lowstock', None) is not None,
            getattr(data, 'stock_status', None) is not None,
        ])

    @classmethod
    async def get_all(
        cls,
        data:GetAllProductSchema
    ) -> List[dict]:
        try:
            query = cls._build_search_query(data)
            has_stock_filter = cls._has_stock_filter(data)

            cursor = PROD_INV_COLLECTION.find(query).sort("created_at", -1)
            
            if not has_stock_filter and getattr(data, 'limit', None):
                offset = data.offset - 1 if (data.offset and data.offset > 0) else 0
                cursor = cursor.skip(offset * data.limit).limit(data.limit)
                data_res = await cursor.to_list(length=None)
                for d in data_res:
                    d["_id"] = str(d["_id"])
                return data_res

            data_res = await cursor.to_list(length=None)
            filtered_res = []
            for d in data_res:
                d["_id"] = str(d["_id"])
                if cls.filter_product_item(d, data):
                    filtered_res.append(d)

            if getattr(data, 'limit', None):
                offset = data.offset - 1 if (data.offset and data.offset > 0) else 0
                start = offset * data.limit
                return filtered_res[start:start + data.limit]

            return filtered_res

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
            has_stock_filter = cls._has_stock_filter(data)

            cursor = PROD_INV_COLLECTION.find(query).sort("created_at", -1)
            
            if not has_stock_filter and getattr(data, 'limit', None):
                offset = data.offset - 1 if (data.offset and data.offset > 0) else 0
                cursor = cursor.skip(offset * data.limit).limit(data.limit)
                data_res = await cursor.to_list(length=None)
                for d in data_res:
                    d["_id"] = str(d["_id"])
                return data_res

            data_res = await cursor.to_list(length=None)
            filtered_res = []
            for d in data_res:
                d["_id"] = str(d["_id"])
                if cls.filter_product_item(d, data):
                    filtered_res.append(d)

            if getattr(data, 'limit', None):
                offset = data.offset - 1 if (data.offset and data.offset > 0) else 0
                start = offset * data.limit
                return filtered_res[start:start + data.limit]

            return filtered_res

        except Exception as e:
            ic(f"Error in get_by_shop_id: {e}")
            return []

    @classmethod
    async def get_by_id(
        cls,
        data:GetProductsById,
    ) -> Optional[dict]:
        try:
            query = cls._build_search_query(data, base_query={"shop_id": data.shop_id, "id": data.id})
            doc = await PROD_INV_COLLECTION.find_one(query)
            if doc:
                doc["_id"] = str(doc["_id"])
                if cls.filter_product_item(doc, data):
                    return doc

            return None

        except Exception as e:
            ic(f"Error in get_by_id: {e}")
            return None


    @classmethod   
    async def get_bulk_by_id(
        cls,
        data:GetBulkProductsById,
    ) -> List[dict]:
        try:
            base_q = {"id": {"$in": data.id}}
            if data.shop_id:
                base_q["shop_id"] = data.shop_id

            query = cls._build_search_query(data, base_query=base_q)

            cursor = PROD_INV_COLLECTION.find(
                query,
                {"_id": 0},
            )

            docs = await cursor.to_list(length=len(data.id))
            return [d for d in docs if cls.filter_product_item(d, data)]

        except Exception as e:
            ic(f"Error in get_bulk_by_id: {e}")
            return []

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

        


