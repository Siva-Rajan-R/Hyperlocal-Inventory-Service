from fastapi import APIRouter,Depends,Query
from ...handlers.stock_adj_handler import HandleStockAdjRequest,StockAdjCreateSchema,StockAdjUpdateSchema,TimeZoneEnum,Optional
from typing import Annotated
from infras.primary_db.main import get_pg_async_session,AsyncSession

router=APIRouter(
    tags=["StockAdjustments CRUD's"],
    prefix='/s-adjustments'
)

PG_ASYNC_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]
SHOP_ID="string"

@router.post('/')
async def create(data:StockAdjCreateSchema,session:PG_ASYNC_SESSION):
    return await HandleStockAdjRequest(session=session).create(data=data)

# @router.put('/')
# async def update(data:StockAdjUpdateSchema,session:PG_ASYNC_SESSION):
#     return await HandleStockAdjRequest(session=session).update(data=data)

@router.delete('/{stock_adj_id}')
async def delete(stock_adj_id:str,session:PG_ASYNC_SESSION):
    return await HandleStockAdjRequest(session=session).delete(stock_adj_id=stock_adj_id,shop_id=SHOP_ID)

@router.get('/')
async def get_all(session:PG_ASYNC_SESSION,q:Optional[str]=Query(''),limit:Optional[int]=Query(10),offset:int=Query(1),timezone:Optional[TimeZoneEnum]=Query(TimeZoneEnum.Asia_Kolkata)):
    return await HandleStockAdjRequest(session=session).get(
        shop_id=SHOP_ID,
        timezone=timezone,
        query=q,
        limit=limit,
        offset=offset,
        full=True
    )

@router.get('/by/{stock_adj_id}')
async def getby_inventory_id(session:PG_ASYNC_SESSION,stock_adj_id:str,timezone:Optional[TimeZoneEnum]=Query(TimeZoneEnum.Asia_Kolkata)):
    return await HandleStockAdjRequest(session=session).getby_id(stock_adj_id=stock_adj_id,shop_id=SHOP_ID,timezone=timezone)
