from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from schemas.v1.request_schemas.customfield_schema import (
    CreateCustomFieldSchema, UpdateCustomFieldSchema,
    CreateCustomFieldValueSchema, UpdateCustomFieldValueSchema,
    BulkCreateCustomFieldValuesSchema,DeleteCustomFieldSchema,GetFieldById,GetFieldByName,GetFieldByShopIdSchema,GetValueByIdName,GetvaluesByProductId
)
from schemas.v1.db_schemas.customfield_schema import CreateCustomFieldDbSchema, CreateCustomFieldValueDbSchema,DeleteCustomFieldDbSchema,UpdateCustomFieldDbSchema
from ..repos.customfield_repo import CustomFieldsRepo
from typing import List,Any

class CustomFieldsService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = CustomFieldsRepo(session)

    async def create_bulk_field(self, data: CreateCustomFieldSchema) -> dict:
        data_toadd=[]
        shop_id=data.shop_id
        names=[field_data.field_name for field_data in data.field_infos]
        
        existing=await self.repo.get_bulk_fields(names=names,shop_id=shop_id)
        existing_names = {f.get("field_name") for f in existing}

        for field_data in data.field_infos:
            if field_data.field_name in existing_names:
                continue
            field_id = generate_uuid()
            f_dump = field_data.model_dump()
            cat_id = f_dump.get("category_id")
            if cat_id in ("ALL", "all", "", "null", None):
                f_dump["category_id"] = None
            data_toadd.append(
                CreateCustomFieldDbSchema(
                    id=field_id,
                    shop_id=shop_id,
                    **f_dump
                )
            )
        
        if data_toadd:
            await self.repo.create_all_field(data=data_toadd)
        return {"success": True}
    async def update_field(self, data: UpdateCustomFieldSchema) -> dict:
        field_id = getattr(data, 'id', None) or getattr(data, 'field_id', None)
        if not field_id:
            raise HTTPException(status_code=400, detail="Field ID is required")
        existing = await self.repo.get_field_by_id(GetFieldById(id=field_id, shop_id=data.shop_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Custom field not found")

        value_exists = await self.repo.get_values_by_field_id(field_id=field_id, shop_id=data.shop_id)
        has_data = any(v.get("value") is not None and str(v.get("value")).strip() != "" for v in value_exists)

        if has_data and data.type and data.type != existing.get("type"):
            raise HTTPException(
                status_code=400,
                detail="Field type cannot be changed because this custom field already contains saved values. Only label name and visibility can be updated."
            )

        update_data = data.model_dump(exclude_unset=True, exclude_none=True, exclude=['field_id', 'id', 'shop_id'])
        if 'category_id' in update_data and update_data['category_id'] in ("ALL", "all", "", "null"):
            update_data['category_id'] = None
        if has_data and 'type' in update_data:
            del update_data['type']

        if not update_data:
            return {"success": True, "message": "No fields to update"}
            
        updated_id = await self.repo.update_field(data=UpdateCustomFieldDbSchema(id=field_id, shop_id=data.shop_id, **update_data))
        if not updated_id:
            raise HTTPException(status_code=500, detail="Failed to update custom field")
            
        return {"success": True, "id": updated_id}

    async def delete_field(self, data: DeleteCustomFieldSchema) -> dict:
        field_id = getattr(data, 'id', None) or getattr(data, 'field_id', None)
        if not field_id:
            raise HTTPException(status_code=400, detail="Field ID is required")
            
        existing = await self.repo.get_field_by_id(GetFieldById(id=field_id, shop_id=data.shop_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Custom field not found")

        field_name = existing.get("field_name")
        res = await self.repo.cascade_delete_field(field_id=field_id, shop_id=data.shop_id, field_name=field_name)
        if not res.get("success"):
            raise HTTPException(status_code=500, detail="Failed to delete custom field")

        affected_product_ids = res.get("affected_product_ids", [])

        # Cascade-clean MongoDB read DB if available
        try:
            from infras.read_db.main import DB
            if DB is not None:
                prod_col = DB["products"]
                unset_doc = {
                    f"custom_fields.{field_id}": "",
                    f"additional_infos.{field_id}": ""
                }
                if field_name:
                    unset_doc[f"custom_fields.{field_name}"] = ""
                    unset_doc[f"additional_infos.{field_name}"] = ""
                await prod_col.update_many(
                    {"shop_id": data.shop_id},
                    {"$unset": unset_doc}
                )
        except Exception as mongo_err:
            from icecream import ic
            ic("Mongo cascade clean error:", mongo_err)

        # Trigger read DB sync for affected products
        if affected_product_ids:
            try:
                from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo
                await ProdInvReadDbRepo.add_updatereaddb(
                    shop_id=data.shop_id,
                    product_ids=affected_product_ids,
                    session=self.session
                )
            except Exception as sync_err:
                from icecream import ic
                ic("Read db sync error after custom field delete:", sync_err)

        return {
            "success": True,
            "message": "Custom field deleted successfully",
            "field_id": field_id,
            "affected_products_count": res.get("usage_count", 0)
        }

    async def get_all_fields(self) -> list:
        return await self.repo.get_fields()

    async def get_field_by_shop_id(self, data: GetFieldByShopIdSchema) -> Any:
        field = await self.repo.get_fields_by_shop_id(data=data)
        if not field:
            if getattr(data, 'paginate', True) is False:
                return []
            return {
                "items": [],
                "total": 0,
                "page": max(1, getattr(data, 'page', 1) or 1),
                "limit": max(1, getattr(data, 'limit', 10) or 10),
                "total_pages": 0,
            }
        return field
    
    async def get_field_by_id(self,data:GetFieldById):
        field = await self.repo.get_field_by_id(data=data)
        if not field:
            return {}
        return field

    # --- Values ---

    async def upsert_values(self, data: CreateCustomFieldValueSchema) -> dict: 
        data_toadd = []  
        shop_id = data.shop_id
        product_id = data.product_id

        raw_items = list(data.value_infos or getattr(data, 'values', None) or [])
        if getattr(data, 'field_id', None) and getattr(data, 'value', None) is not None:
            raw_items.append({"field_id": data.field_id, "value": data.value})

        if not raw_items:
            return {"success": True}

        # Validate that field_id actually exists in custom_fields table for this shop
        existing_res = await self.repo.get_fields_by_shop_id(GetFieldByShopIdSchema(shop_id=shop_id, paginate=False))
        existing_fields = existing_res.get("items", []) if isinstance(existing_res, dict) else (existing_res or [])
        fields_by_id = {f.get("id"): f for f in existing_fields} if existing_fields else {}
        # Also map field_name to field in case field_name was passed
        fields_by_name = {f.get("field_name"): f for f in existing_fields} if existing_fields else {}

        import json
        for d in raw_items:
            raw_f_id = getattr(d, 'field_id', None) if not isinstance(d, dict) else d.get('field_id')
            f_val = getattr(d, 'value', None) if not isinstance(d, dict) else d.get('value')
            
            matched_field = fields_by_id.get(raw_f_id) or fields_by_name.get(raw_f_id)
            if not matched_field:
                continue

            f_id = matched_field.get("id")
            field_type = str(matched_field.get("type", "")).lower()

            # Ensure value is converted to a string format suitable for the db column
            if field_type in ("boolean", "bool", "checkbox") or isinstance(f_val, bool):
                if isinstance(f_val, bool):
                    f_val_str = "true" if f_val else "false"
                elif str(f_val).strip().lower() in ("true", "1", "yes"):
                    f_val_str = "true"
                else:
                    f_val_str = "false"
            elif isinstance(f_val, (dict, list)):
                f_val_str = json.dumps(f_val)
            else:
                f_val_str = str(f_val) if f_val is not None else ""

            value_id = generate_uuid()
            data_toadd.append(CreateCustomFieldValueDbSchema(
                id=value_id,
                shop_id=shop_id,
                product_id=product_id,
                field_id=f_id,
                value=f_val_str
            ))
        
        if data_toadd:
            await self.repo.upsert_field_value(data=data_toadd)

            # Also sync custom field values into Products.additional_infos for immediate consistency
            try:
                from infras.primary_db.models.product_model import Products
                from sqlalchemy import select, update
                p_stmt = select(Products.additional_infos).where(Products.id == product_id, Products.shop_id == shop_id)
                existing_add = (await self.session.execute(p_stmt)).scalar_one_or_none() or {}
                if not isinstance(existing_add, dict):
                    existing_add = {}
                new_add = dict(existing_add)
                for item in data_toadd:
                    m_f = fields_by_id.get(item.field_id)
                    new_add[item.field_id] = item.value
                    if m_f and m_f.get("field_name"):
                        new_add[m_f["field_name"]] = item.value
                up_stmt = update(Products).where(Products.id == product_id, Products.shop_id == shop_id).values(additional_infos=new_add)
                await self.session.execute(up_stmt)
                try:
                    await self.session.commit()
                except Exception:
                    await self.session.flush()

                # Sync custom fields to Read DB Pipeline for real-time consistency
                try:
                    from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo
                    await ProdInvReadDbRepo.add_updatereaddb(
                        shop_id=shop_id,
                        product_ids=[product_id],
                        session=self.session
                    )
                except Exception as sync_read_err:
                    from icecream import ic
                    ic("Failed to sync read db in customfield_service:", sync_read_err)
            except Exception as sync_err:
                from icecream import ic
                ic("Failed to sync additional_infos on products table:", sync_err)

        return {"success": True}

    async def bulk_upsert_values(self, data: Any) -> dict:
        if isinstance(data, CreateCustomFieldValueSchema):
            return await self.upsert_values(data=data)
        schema_data = CreateCustomFieldValueSchema(
            shop_id=getattr(data, 'shop_id', ''),
            product_id=getattr(data, 'product_id', ''),
            value_infos=getattr(data, 'values', []) or getattr(data, 'value_infos', [])
        )
        return await self.upsert_values(data=schema_data)

    async def get_values_by_purchase(self,data:GetvaluesByProductId) -> list:
        return await self.repo.get_values_by_purchase_id(data=data)

    async def get_values_by_product(self, product_id: str, shop_id: str) -> list:
        return await self.repo.get_values_by_product_id(
            data=GetvaluesByProductId(id=product_id, shop_id=shop_id)
        )
