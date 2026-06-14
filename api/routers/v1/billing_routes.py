from fastapi import APIRouter,Depends,Query
from ...handlers.billing_handler import CreateBillingSchema,HandleBillingRequest
from schemas.v1.request_schemas.billing_schema import CreateBillingReturnSchema,CreateBillingExchangeSchema,CreateBillingReturnBulkSchema,CreateBillingBulkExchangeSchema
from infras.primary_db.main import get_pg_async_session,AsyncSession
from typing import Annotated



router=APIRouter(
    tags=["Billing Crud's"],
    prefix='/billing'
)

ASYNC_PG_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post('')
async def create_billing(data:CreateBillingSchema,session:ASYNC_PG_SESSION):
    return await HandleBillingRequest(session=session).create_v2(data=data)

@router.post('/return')
async def create_billing(data:CreateBillingReturnBulkSchema,session:ASYNC_PG_SESSION):
    return await HandleBillingRequest(session=session).return_order_bulk(data=data)

@router.post('/exchange')
async def create_billing(data:CreateBillingBulkExchangeSchema,session:ASYNC_PG_SESSION):
    return await HandleBillingRequest(session=session).exchange_order_bulk(data=data)

@router.get('/stats')
async def get_billing_stats(shop_id: str = Query(...), session: ASYNC_PG_SESSION = None):
    return await HandleBillingRequest(session=session).get_billing_stats(shop_id=shop_id)

@router.get('')
async def get_billings(shop_id: str = Query(...), limit: int = Query(50), skip: int = Query(0), session: ASYNC_PG_SESSION = None):
    return await HandleBillingRequest(session=session).get_billings(shop_id=shop_id, limit=limit, skip=skip)