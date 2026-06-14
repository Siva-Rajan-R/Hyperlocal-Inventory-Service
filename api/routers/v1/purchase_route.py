from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from ...handlers.purchase_handler import HandlePurchaseRequest
from fastapi import APIRouter,Query,Depends
from infras.primary_db.main import AsyncSession,get_pg_async_session
from typing import Optional,Annotated,List
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,GetPurchaseByShopIdSchema,GetPurchaseByIdSchema,GetPurchaseByInventoryIdSchema,GetPurchaseBySupplierIdSchema
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums,PurchaseViewsEnums


router=APIRouter(
    tags=["Purchase Crud's"],
    prefix="/purchases"
)

SHOP_ID="37d5519b-51a1-5854-982b-4d6524171017"
ADDED_BY="siva-user"

ASYNC_PG_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post("")
async def create(data:CreatePurchaseSchema,session:ASYNC_PG_SESSION):
    return await HandlePurchaseRequest(session=session).create(data=data)


@router.put("")
async def update(data:CreatePurchaseSchema,session:ASYNC_PG_SESSION):
    return await HandlePurchaseRequest(session=session).update(data=data,user_id=ADDED_BY)

@router.put("/{purchase_id}")
async def update_by_id(purchase_id:str,data:CreatePurchaseSchema,session:ASYNC_PG_SESSION):
    data.purchase_id = purchase_id
    return await HandlePurchaseRequest(session=session).update(data=data,user_id=ADDED_BY)


@router.delete("/{purchase_id}")
async def delete(purchase_id:str,session:ASYNC_PG_SESSION):
    return await HandlePurchaseRequest(session=session).delete(shop_id=SHOP_ID,id=purchase_id)


@router.get("")
async def get(session:ASYNC_PG_SESSION,data:GetPurchaseByShopIdSchema=Depends()):
    return await HandlePurchaseRequest(session=session).get(data=data)

@router.get("/by/{shop_id}/{id}")
async def getby_id(session:ASYNC_PG_SESSION,data:GetPurchaseByIdSchema=Depends()):
    return await HandlePurchaseRequest(session=session).getby_id(data=data)

@router.get("/by/product/{shop_id}/{inventory_id}")
async def getby_inventory_id(session:ASYNC_PG_SESSION,data:GetPurchaseByInventoryIdSchema=Depends()):
    return await HandlePurchaseRequest(session=session).get_by_inventory_id(data=data)

@router.get("/by/supplier/{shop_id}/{supplier_id}")
async def getby_inventory_id(session:ASYNC_PG_SESSION,data:GetPurchaseBySupplierIdSchema=Depends()):
    return await HandlePurchaseRequest(session=session).getby_supplier_id(data=data)

@router.get("/search/{shop_id}")
async def search(shop_id: str, session:ASYNC_PG_SESSION, q: str = Query(""), limit: int = Query(10, ge=1, le=50)):
    return await HandlePurchaseRequest(session=session).search(shop_id=shop_id, query=q, limit=limit)

@router.get("/stats/supplier/{shop_id}/{supplier_id}")
async def get_supplier_stats(shop_id: str, supplier_id: str, session:ASYNC_PG_SESSION):
    return await HandlePurchaseRequest(session=session).get_supplier_stats(shop_id=shop_id, supplier_id=supplier_id)