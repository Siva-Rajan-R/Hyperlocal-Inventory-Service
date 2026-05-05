from fastapi import APIRouter,Depends,Query
from schemas.v1.request_schemas.inventory_schema import CreateInventorySchema,UpdateInventorySchema,DeleteInventorySchema,GetAllInventorySchema,GetInventoryByIdSchema,GetInventoryByShopIdSchema,VerifySchema
from ...handlers.inventory_handler import HandleInventoryRequest
from typing import Annotated,Optional
from infras.primary_db.main import get_pg_async_session,AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum

router=APIRouter(
    tags=["Inventory CRUD's"],
    prefix='/inventories'
)

PG_ASYNC_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post('')
async def create(data:CreateInventorySchema,session:PG_ASYNC_SESSION):
    return await HandleInventoryRequest(session=session).create(data=data)

@router.put('')
async def update(data:UpdateInventorySchema,session:PG_ASYNC_SESSION):
    return await HandleInventoryRequest(session=session).update(data=data)

@router.delete('/{shop_id}/{id}')
async def delete(session:PG_ASYNC_SESSION,data:DeleteInventorySchema=Depends()):
    return await HandleInventoryRequest(session=session).delete(data=data)

@router.get('')
async def get_all(session:PG_ASYNC_SESSION,data:GetAllInventorySchema=Depends()):
    return await HandleInventoryRequest(session=session).get(data=data)

@router.get('/by/shop/{shop_id}')
async def getby_shop_id(session:PG_ASYNC_SESSION,data:GetInventoryByShopIdSchema=Depends()):
    return await HandleInventoryRequest(session=session).getby_shop_id(data=data)

@router.get('/by/{shop_id}/{id}')
async def getby_id(session:PG_ASYNC_SESSION,data:GetInventoryByIdSchema=Depends()):
    return await HandleInventoryRequest(session=session).getby_id(data=data)
