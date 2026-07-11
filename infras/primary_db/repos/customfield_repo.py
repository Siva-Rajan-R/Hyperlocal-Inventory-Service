from sqlalchemy import select, update, delete,or_,and_,bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
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
        return res

    @start_db_transaction
    async def delete_field(self,data:DeleteCustomFieldDbSchema) -> bool:
        stmt = delete(ProductCustomFields).where(
            ProductCustomFields.id == data.id,
            ProductCustomFields.shop_id == data.shop_id
        )
        res = await self.session.execute(stmt)
        return res.rowcount > 0


    async def get_field_by_id(self,data:GetFieldById) -> Optional[dict]:
        stmt = select(ProductCustomFields).where(
            ProductCustomFields.id == data.id, 
            ProductCustomFields.shop_id == data.shop_id
        )
        res = (await self.session.execute(stmt)).scalars().first()
        if res:
            return {c.name: getattr(res, c.name) for c in res.__table__.columns}
        return None
    

    async def get_bulk_fields(self,shop_id: str,ids: List[str]=[],names:List[str]=[]) -> Optional[dict]:
        if not ids and not names:
            return []
        
        stmt = select(ProductCustomFields).where(
            or_(ProductCustomFields.id.in_(ids),ProductCustomFields.field_name.in_(names)), 
            ProductCustomFields.shop_id == shop_id
        )
        res = (await self.session.execute(stmt)).mappings().all()
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
            return {c.name: getattr(res, c.name) for c in res.__table__.columns}
        return None

    async def get_fields_by_shop_id(self, data:GetFieldByShopIdSchema) -> List[dict]:
        stmt = select(ProductCustomFields).where(ProductCustomFields.shop_id == data.shop_id)
        res = (await self.session.execute(stmt)).scalars().all()
        return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in res]
    

    async def get_fields(self) -> List[dict]:
        stmt = select(ProductCustomFields)
        res = (await self.session.execute(stmt)).scalars().all()
        return [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in res]


    @start_db_transaction
    async def upsert_field_value(self, data: List[CreateCustomFieldValueDbSchema]) -> bool:
        if not data:
            return True

        # 1. Convert the pydantic schemas to a list of raw dictionaries
        insert_mappings = [d.model_dump() for d in data]

        # 2. Build the native PostgreSQL INSERT statement
        stmt = insert(ProductCustomFieldsValues)
        
        # 3. Construct the UPSERT (ON CONFLICT) logic
        # Resolves on the unique combination of customer and field
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["product_id", "field_id"],  
            set_={
                "value": stmt.excluded.value,   # Update the actual text/data value
                "shop_id": stmt.excluded.shop_id  # Keeps the shop relation intact/valid
            }
        )

        # 4. Execute the batch operation efficiently in one database round-trip
        conn = await self.session.connection()
        res = await conn.execute(upsert_stmt, insert_mappings)
        
        ic("Total rows handled (Inserted + Updated) => ", res.rowcount)
        return True

        
    async def get_values_by_product_id(self, data:GetvaluesByProductId) -> List[dict]:
        stmt = (
            select(
                ProductCustomFieldsValues.id,
                ProductCustomFieldsValues.shop_id,
                ProductCustomFieldsValues.product_id,
                ProductCustomFieldsValues.field_id,
                ProductCustomFieldsValues.value,
                ProductCustomFields.field_name
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

