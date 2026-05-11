from fastapi import APIRouter,Depends,Query
from ...handlers.billing_handler import CreateBillingSchema,HandleBillingRequest
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