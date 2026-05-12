from fastapi import APIRouter,Depends,Query
from ...handlers.billing_handler import CreateBillingSchema,HandleBillingRequest
from schemas.v1.request_schemas.billing_schema import CreateBillingReturnSchema,CreateBillingExchangeSchema
from infras.primary_db.main import get_pg_async_session,AsyncSession
from typing import Annotated



router=APIRouter(
    tags=["Billing Crud's"],
    prefix='/billing'
)

ASYNC_PG_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post('')
async def create_billing(data:CreateBillingSchema,session:ASYNC_PG_SESSION):
    return await HandleBillingRequest(session=session).create(data=data)

@router.post('/return')
async def create_billing(data:CreateBillingReturnSchema,session:ASYNC_PG_SESSION):
    return await HandleBillingRequest(session=session).return_order(data=data)

@router.post('/exchange')
async def create_billing(data:CreateBillingExchangeSchema,session:ASYNC_PG_SESSION):
    return await HandleBillingRequest(session=session).exchange_order(data=data)