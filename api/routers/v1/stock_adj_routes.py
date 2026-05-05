from fastapi import APIRouter,Depends,Query
from ...handlers.stock_adj_handler import HandleStockAdjRequest,CreateStockAdjSchema,TimeZoneEnum,Optional,GetStockAdjByShopIdSchema,GetStockAdjByIdSchema,GetAllStockAdjSchema,GetStockAdjByInventoryIdSchema
from typing import Annotated
from infras.primary_db.main import get_pg_async_session,AsyncSession

router=APIRouter(
    tags=["StockAdjustments CRUD's"],
    prefix='/s-adjustments'
)

PG_ASYNC_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]
SHOP_ID="37d5519b-51a1-5854-982b-4d6524171017"

@router.post('')
async def create(data:CreateStockAdjSchema,session:PG_ASYNC_SESSION):
    return await HandleStockAdjRequest(session=session).create(data=data)

# @router.put('/')
# async def update(data:StockAdjUpdateSchema,session:PG_ASYNC_SESSION):
#     return await HandleStockAdjRequest(session=session).update(data=data)

@router.delete('/{stock_adj_id}')
async def delete(stock_adj_id:str,session:PG_ASYNC_SESSION):
    return await HandleStockAdjRequest(session=session).delete(stock_adj_id=stock_adj_id,shop_id=SHOP_ID)

@router.get('')
async def get_all(session:PG_ASYNC_SESSION,data:GetAllStockAdjSchema=Depends()):
    return await HandleStockAdjRequest(session=session).get(data=data)

@router.get('/by/shop/{shop_id}')
async def get_all(session:PG_ASYNC_SESSION,data:GetStockAdjByShopIdSchema=Depends()):
    return await HandleStockAdjRequest(session=session).getby_shop_id(data=data)

@router.get('/by/{shop_id}/{stock_adj_id}')
async def getby_inventory_id(session:PG_ASYNC_SESSION,data:GetStockAdjByIdSchema=Depends()):
    return await HandleStockAdjRequest(session=session).getby_id(data=data)

@router.get('/by/product/{shop_id}/{inventory_id}')
async def getby_inventory_id(session:PG_ASYNC_SESSION,data:GetStockAdjByInventoryIdSchema=Depends()):
    return await HandleStockAdjRequest(session=session).getby_inventory_id(data=data)
