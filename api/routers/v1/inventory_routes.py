from fastapi import APIRouter,Depends,Query
from ...handlers.inventory_handler import HandleInventoryRequest,AddInventorySchema,UpdateInventorySchema,TimeZoneEnum,Optional
from typing import Annotated
from infras.primary_db.main import get_pg_async_session,AsyncSession

router=APIRouter(
    tags=["Inventory CRUD's"],
    prefix='/inventories'
)

PG_ASYNC_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post('')
async def create(data:AddInventorySchema,session:PG_ASYNC_SESSION):
    return await HandleInventoryRequest(session=session).create(data=data,account_id="")

@router.put('')
async def update(data:UpdateInventorySchema,session:PG_ASYNC_SESSION):
    return await HandleInventoryRequest(session=session).update(data=data,account_id="")

@router.delete('/{inventory_id}/{shop_id}')
async def delete(inventory_id:str,shop_id:str,session:PG_ASYNC_SESSION):
    return await HandleInventoryRequest(session=session).delete(inventory_id=inventory_id,shop_id=shop_id)

@router.get('')
async def get_all(session:PG_ASYNC_SESSION,shop_id:str=Query(...),q:Optional[str]=Query(''),limit:Optional[int]=Query(10),offset:int=Query(1),timezone:Optional[TimeZoneEnum]=Query(TimeZoneEnum.Asia_Kolkata)):
    return await HandleInventoryRequest(session=session).get(
        shop_id=shop_id,
        timezone=timezone,
        query=q,
        limit=limit,
        offset=offset,
        read_db=False
    )

@router.get('/by/{inventory_id}/{shop_id}')
async def getby_inventory_id(session:PG_ASYNC_SESSION,inventory_id:str,shop_id:str,timezone:Optional[TimeZoneEnum]=Query(TimeZoneEnum.Asia_Kolkata)):
    return await HandleInventoryRequest(session=session).getby_id(inventory_id=inventory_id,shop_id=shop_id,timezone=timezone,read_db=False)
