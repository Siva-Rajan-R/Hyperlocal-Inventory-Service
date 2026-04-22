from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from ...handlers.purchase_handler import HandlePurchaseRequest
from fastapi import APIRouter,Query,Depends
from infras.primary_db.main import AsyncSession,get_pg_async_session
from typing import Optional,Annotated,List
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,UpdatePurchaseSchema
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums


router=APIRouter(
    tags=["Purchase Crud's"],
    prefix="/purchases"
)

SHOP_ID="37d5519b-51a1-5854-982b-4d6524171017"
ADDED_BY="siva-user"

ASYNC_PG_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post("")
async def create(data:CreatePurchaseSchema,session:ASYNC_PG_SESSION):
    return await HandlePurchaseRequest(session=session).create(data=data,added_by=ADDED_BY,shop_id=SHOP_ID)


@router.put("")
async def update(data:UpdatePurchaseSchema,session:ASYNC_PG_SESSION):
    return await HandlePurchaseRequest(session=session).update(data=data,user_id=ADDED_BY)


@router.delete("/{purchase_id}")
async def delete(purchase_id:str,session:ASYNC_PG_SESSION):
    return await HandlePurchaseRequest(session=session).delete(shop_id=SHOP_ID,id=purchase_id)


@router.get("")
async def get(session:ASYNC_PG_SESSION,timezone:Optional[TimeZoneEnum]=Query(TimeZoneEnum.Asia_Kolkata),type:PurchaseTypeEnums=Query(...),q:Optional[str]="",limit:Optional[int]=10,offset:int=1):
    return await HandlePurchaseRequest(session=session).get(shop_id=SHOP_ID,timezone=timezone,query=q,limit=limit,offset=offset,type=type)