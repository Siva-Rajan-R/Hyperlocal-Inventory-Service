from sqlalchemy import select, update, delete,or_,and_,bindparam,func
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional, Union
import math
from icecream import ic
from sqlalchemy.dialects.postgresql import insert
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from ..models.customfield_model import ProductCustomFields,ProductCustomFieldsValues
from schemas.v1.db_schemas.customfield_schema import CreateCustomFieldDbSchema, CreateCustomFieldValueDbSchema,UpdateCustomFieldDbSchema,DeleteCustomFieldDbSchema
from schemas.v1.request_schemas.customfield_schema import GetFieldById,GetFieldByShopIdSchema,GetFieldByName,GetValueByIdName,GetvaluesByProductId

class CustomFieldsRepo:
    def __init__(self, session: AsyncSession):
        self.session = session

    # --- Custom Fields (Definitions) ---
    
    @start_db_transaction
    async def create_all_field(self, data: List[CreateCustomFieldDbSchema]) -> bool:
        field_toadd=[ProductCustomFields(**field.model_dump()) for field in data]
        self.session.add_all(field_toadd)
        await self.session.commit()
        return True
    

    @start_db_transaction
    async def update_field(self, data:UpdateCustomFieldDbSchema) -> Optional[str]:
        stmt = (
            update(ProductCustomFields)
            .where(ProductCustomFields.id == data.id, ProductCustomFields.shop_id == data.shop_id)
            .values(**data.model_dump(exclude=['field_id','shop_id'],exclude_none=True,exclude_unset=True))
            .returning(ProductCustomFields.id)
        )
        res = (await self.session.execute(stmt)).scalar_one_or_none()
        await self.session.commit()
        return res

    @start_db_transaction
    async def delete_field(self, data: DeleteCustomFieldDbSchema) -> bool:
        target_id = data.id if hasattr(data, 'id') and data.id else getattr(data, 'field_id', None)
        stmt = delete(ProductCustomFields).where(
            ProductCustomFields.id == target_id,
            ProductCustomFields.shop_id == data.shop_id
        )
        res = await self.session.execute(stmt)
        await self.session.commit()
        return res.rowcount > 0


    async def get_field_by_id(self,data:GetFieldById) -> Optional[dict]:
        stmt = select(ProductCustomFields).where(
            ProductCustomFields.id == data.id, 
            ProductCustomFields.shop_id == data.shop_id
        )
        res = (await self.session.execute(stmt)).scalars().first()
        if res:
            d = {c.name: getattr(res, c.name) for c in res.__table__.columns}
            val_stmt = select(func.count(func.distinct(ProductCustomFieldsValues.product_id))).where(
                ProductCustomFieldsValues.field_id == data.id,
                ProductCustomFieldsValues.shop_id == data.shop_id,
                ProductCustomFieldsValues.value.isnot(None),
                ProductCustomFieldsValues.value != ""
            )
            cnt = (await self.session.execute(val_stmt)).scalar() or 0
            d["usage_count"] = int(cnt)
            d["has_values"] = int(cnt) > 0
            return d
        return None
    

    async def get_bulk_fields(self,shop_id: str,ids: List[str]=[],names:List[str]=[]) -> List[dict]:
        if not ids and not names:
            return []
        
        stmt = select(ProductCustomFields).where(
            or_(ProductCustomFields.id.in_(ids),ProductCustomFields.field_name.in_(names)), 
            ProductCustomFields.shop_id == shop_id
        )
        res = (await self.session.execute(stmt)).scalars().all()
        if res:
            return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in res]
        return []
    

    async def get_field_by_name(self, data:GetFieldByName) -> Optional[dict]:
        stmt = select(ProductCustomFields).where(
            ProductCustomFields.field_name == data.name, 
            ProductCustomFields.shop_id == data.shop_id
        )
        res = (await self.session.execute(stmt)).scalars().first()
        if res:
            return {c.name: getattr(res, c.name) for c in row.__table__.columns}
        return None

    async def get_fields_by_shop_id(self, data: GetFieldByShopIdSchema) -> Union[dict, List[dict]]:
        stmt = select(ProductCustomFields).where(ProductCustomFields.shop_id == data.shop_id)

        # 1. Category filtering: if "global", category_id IS NULL; else category_id = :category_id
        cat_id = getattr(data, 'category_id', None)
        if cat_id and str(cat_id).strip().lower() not in ("all", ""):
            cat_lower = str(cat_id).strip().lower()
            if cat_lower in ("global", "all_categories_only", "none", "null"):
                stmt = stmt.where(
                    or_(
                        ProductCustomFields.category_id.is_(None),
                        ProductCustomFields.category_id == ""
                    )
                )
            else:
                stmt = stmt.where(ProductCustomFields.category_id == str(cat_id).strip())

        # 2. Field type filtering
        field_type = getattr(data, 'field_type', None)
        if field_type and str(field_type).strip().lower() not in ("all", ""):
            stmt = stmt.where(func.lower(ProductCustomFields.type) == str(field_type).strip().lower())

        # 3. Search filtering: matching field_label ILIKE :search or field_name ILIKE :search
        search = getattr(data, 'search', None)
        if search and str(search).strip():
            search_term = f"%{str(search).strip()}%"
            stmt = stmt.where(
                or_(
                    ProductCustomFields.label_name.ilike(search_term),
                    ProductCustomFields.field_name.ilike(search_term)
                )
            )

        should_paginate = getattr(data, 'paginate', True)
        if not should_paginate:
            stmt = stmt.order_by(ProductCustomFields.created_at.desc())
            res = (await self.session.execute(stmt)).scalars().all()
            field_ids = [row.id for row in res]
            usage_counts = {}
            if field_ids:
                val_stmt = (
                    select(
                        ProductCustomFieldsValues.field_id,
                        func.count(func.distinct(ProductCustomFieldsValues.product_id)).label("count")
                    )
                    .where(
                        ProductCustomFieldsValues.field_id.in_(field_ids),
                        ProductCustomFieldsValues.shop_id == data.shop_id,
                        ProductCustomFieldsValues.value.isnot(None),
                        ProductCustomFieldsValues.value != ""
                    )
                    .group_by(ProductCustomFieldsValues.field_id)
                )
                val_res = (await self.session.execute(val_stmt)).all()
                for r in val_res:
                    usage_counts[str(r[0])] = int(r[1])

            result = []
            for row in res:
                d = {c.name: getattr(row, c.name) for c in row.__table__.columns}
                cnt = usage_counts.get(str(row.id), 0)
                d["usage_count"] = cnt
                d["has_values"] = cnt > 0
                result.append(d)
            return result

        page = max(1, getattr(data, 'page', 1) or 1)
        limit = max(1, min(100, getattr(data, 'limit', 10) or 10))

        # Count total records matching filters
        count_stmt = select(func.count()).select_from(stmt.order_by(None).subquery())
        total = (await self.session.execute(count_stmt)).scalar() or 0
        total_pages = math.ceil(total / limit) if total > 0 else 0

        # Paginated results ordered by creation time descending
        paginated_stmt = stmt.order_by(ProductCustomFields.created_at.desc()).offset((page - 1) * limit).limit(limit)
        res = (await self.session.execute(paginated_stmt)).scalars().all()

        field_ids = [row.id for row in res]
        usage_counts = {}
        if field_ids:
            val_stmt = (
                select(
                    ProductCustomFieldsValues.field_id,
                    func.count(func.distinct(ProductCustomFieldsValues.product_id)).label("count")
                )
                .where(
                    ProductCustomFieldsValues.field_id.in_(field_ids),
                    ProductCustomFieldsValues.shop_id == data.shop_id,
                    ProductCustomFieldsValues.value.isnot(None),
                    ProductCustomFieldsValues.value != ""
                )
                .group_by(ProductCustomFieldsValues.field_id)
            )
            val_res = (await self.session.execute(val_stmt)).all()
            for r in val_res:
                usage_counts[str(r[0])] = int(r[1])

        result = []
        for row in res:
            d = {c.name: getattr(row, c.name) for c in row.__table__.columns}
            cnt = usage_counts.get(str(row.id), 0)
            d["usage_count"] = cnt
            d["has_values"] = cnt > 0
            result.append(d)

        return {
            "items": result,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
        }

    @start_db_transaction
    async def cascade_delete_field(self, field_id: str, shop_id: str, field_name: Optional[str] = None) -> dict:
        from infras.primary_db.models.product_model import Products
        # 1. Find all affected product_ids having saved values for this field
        stmt_pids = select(func.distinct(ProductCustomFieldsValues.product_id)).where(
            ProductCustomFieldsValues.field_id == field_id,
            ProductCustomFieldsValues.shop_id == shop_id,
            ProductCustomFieldsValues.value.isnot(None),
            ProductCustomFieldsValues.value != ""
        )
        affected_product_ids = [str(pid) for pid in (await self.session.execute(stmt_pids)).scalars().all()]

        # 2. Delete all records from product_custom_fields_values for this field
        del_vals_stmt = delete(ProductCustomFieldsValues).where(
            ProductCustomFieldsValues.field_id == field_id,
            ProductCustomFieldsValues.shop_id == shop_id
        )
        await self.session.execute(del_vals_stmt)

        # 3. Delete the custom field definition from product_custom_fields
        del_field_stmt = delete(ProductCustomFields).where(
            ProductCustomFields.id == field_id,
            ProductCustomFields.shop_id == shop_id
        )
        res_del = await self.session.execute(del_field_stmt)

        # 4. In PostgreSQL Products table: Clean additional_infos
        if affected_product_ids:
            p_stmt = select(Products).where(
                Products.id.in_(affected_product_ids),
                Products.shop_id == shop_id
            )
            prods = (await self.session.execute(p_stmt)).scalars().all()
            for prod in prods:
                if isinstance(prod.additional_infos, dict):
                    new_add = dict(prod.additional_infos)
                    new_add.pop(field_id, None)
                    if field_name:
                        new_add.pop(field_name, None)
                    prod.additional_infos = new_add

        await self.session.commit()
        return {
            "success": res_del.rowcount > 0,
            "affected_product_ids": affected_product_ids,
            "usage_count": len(affected_product_ids)
        }
    

    async def get_fields(self) -> List[dict]:
        stmt = select(ProductCustomFields)
        res = (await self.session.execute(stmt)).scalars().all()
        return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in res]


    async def upsert_field_value(self, data: List[CreateCustomFieldValueDbSchema]) -> bool:
        if not data:
            return True

        # 1. Convert the pydantic schemas to a list of raw dictionaries
        insert_mappings = [d.model_dump() for d in data]

        # 2. Build the native PostgreSQL INSERT statement
        stmt = insert(ProductCustomFieldsValues)
        
        # 3. Construct the UPSERT (ON CONFLICT) logic
        # Resolves on the unique combination of product and field
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["product_id", "field_id"],  
            set_={
                "value": stmt.excluded.value,   # Update the actual text/data value
                "shop_id": stmt.excluded.shop_id  # Keeps the shop relation intact/valid
            }
        )

        # 4. Execute the batch operation efficiently in one database round-trip
        res = await self.session.execute(upsert_stmt, insert_mappings)
        try:
            await self.session.commit()
        except Exception:
            await self.session.flush()
        
        ic("Total rows handled (Inserted + Updated) => ", getattr(res, "rowcount", None))
        return True

        
    async def get_values_by_product_id(self, data:GetvaluesByProductId) -> List[dict]:
        stmt = (
            select(
                ProductCustomFieldsValues.id,
                ProductCustomFieldsValues.shop_id,
                ProductCustomFieldsValues.product_id,
                ProductCustomFieldsValues.field_id,
                ProductCustomFieldsValues.value,
                ProductCustomFields.field_name,
                ProductCustomFields.label_name,
                ProductCustomFields.type,
                ProductCustomFields.required,
                ProductCustomFields.visible_online,
                ProductCustomFields.category_id,
            )
            .join(ProductCustomFields, ProductCustomFields.id == ProductCustomFieldsValues.field_id)
            .where(
                ProductCustomFieldsValues.product_id == data.id,
                ProductCustomFieldsValues.shop_id == data.shop_id
            )
        )
        res = (await self.session.execute(stmt)).mappings().all()
        return [dict(row) for row in res]

    async def get_values(self):
        stmt = select(ProductCustomFieldsValues)
        res = (await self.session.execute(stmt)).scalars().all()
        return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in res]
    
    async def get_values_by_id(self,id:str,shop_id:str):
        stmt = select(ProductCustomFieldsValues).where(
            ProductCustomFieldsValues.id == id,
            ProductCustomFieldsValues.shop_id == shop_id
        )
        res = (await self.session.execute(stmt)).mappings().all()
        return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in res]


    async def get_values_by_field_id(self, field_id: str, shop_id: str) -> List[dict]:
        stmt = select(ProductCustomFieldsValues).where(
            ProductCustomFieldsValues.field_id == field_id,
            ProductCustomFieldsValues.shop_id == shop_id
        )
        res = (await self.session.execute(stmt)).scalars().all()
        return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in res]
