from fastapi import APIRouter, Depends
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
from infras.primary_db.main import get_pg_async_session
from schemas.v1.request_schemas.customfield_schema import (
    CreateCustomFieldSchema, UpdateCustomFieldSchema,
    CreateCustomFieldValueSchema, BulkCreateCustomFieldValuesSchema,GetFieldById,GetFieldByName,GetFieldByShopIdSchema,GetValueByIdName,GetvaluesByProductId,DeleteCustomFieldSchema
)
from api.handlers.customfield_handler import CustomFieldsHandler

router = APIRouter(prefix="/custom-fields", tags=["Custom Fields"])

PG_ASYNC_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post("")
async def create_field(data: CreateCustomFieldSchema, session:PG_ASYNC_SESSION):
    return await CustomFieldsHandler.create_field(data=data, session=session)

@router.get("/{shop_id}")
async def get_all_fields(shop_id: str, session:PG_ASYNC_SESSION):
    return await CustomFieldsHandler.get_field_by_shop_id(data=GetFieldByShopIdSchema(shop_id=shop_id), session=session)

@router.get("/{shop_id}/{field_id}")
async def get_field(field_id: str, shop_id: str, session:PG_ASYNC_SESSION):
    return await CustomFieldsHandler.get_field_by_id(data=GetFieldById(shop_id=shop_id,id=field_id), session=session)

@router.put("")
async def update_field(data: UpdateCustomFieldSchema, session:PG_ASYNC_SESSION):
    return await CustomFieldsHandler.update_field(data=data, session=session)

@router.delete("/{shop_id}/{field_id}")
async def delete_field(field_id: str, shop_id: str, session:PG_ASYNC_SESSION):
    return await CustomFieldsHandler.delete_field(field_id=field_id, shop_id=shop_id, session=session)


@router.post("/values")
async def upsert_value(data: CreateCustomFieldValueSchema, session:PG_ASYNC_SESSION):
    return await CustomFieldsHandler.upsert_value(data=data, session=session)

@router.get("/values/{shop_id}/{product_id}")
async def get_values_by_product(product_id: str, shop_id: str, session:PG_ASYNC_SESSION):
    return await CustomFieldsHandler.get_values_by_product(data=GetvaluesByProductId(shop_id=shop_id,id=product_id), session=session)
