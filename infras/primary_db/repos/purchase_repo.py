from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.purchase_model import Purchase
from schemas.v1.db_schemas.purchase_schema import CreatePurchaseDbSchema,UpdatePurchaseDbSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional
from icecream import ic
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums


class PurchaseRepo(BaseRepoModel):
    def __init__(self, session:AsyncSession):
        self.session=session
        self.purchase_cols=(
            Purchase.id,
            Purchase.shop_id,
            Purchase.added_by,
            Purchase.datas,
            Purchase.type,
            Purchase.purchase_view
        )

    @start_db_transaction
    async def create(self,data:CreatePurchaseDbSchema):
        data_toadd=Purchase(**data.model_dump(mode='json'))
        self.session.add(data_toadd)
        return True
    
    @start_db_transaction
    async def update(self,data:UpdatePurchaseDbSchema):
        data_toupdate=update(
            Purchase
        ).where(
            Purchase.id==data.id,
            Purchase.shop_id==data.shop_id
        ).values(
            datas=data.datas,
            purchase_view=data.purchase_view,
        ).returning(Purchase.id)

        is_updated=(await self.session.execute(data_toupdate)).scalar_one_or_none()

        return is_updated
    
    @start_db_transaction
    async def delete(self,id:str,shop_id:str):
        data_todel=delete(
            Purchase
        ).where(Purchase.id==id,Purchase.shop_id==shop_id).returning(Purchase.id)

        is_deleted=(await self.session.execute(data_todel)).scalar_one_or_none()

        return is_deleted

    

    async def get(self,timezone:TimeZoneEnum,shop_id:str,type:PurchaseTypeEnums,query:Optional[str]="",limit:Optional[int]=None,offset:Optional[int]=None):
        created_at=func.date(func.timezone(timezone.value,Purchase.created_at))
        query_stmt=select(
            *self.purchase_cols,
            created_at
        ).where(
            Purchase.type==type.value,
            Purchase.shop_id==shop_id
        )

        if offset is not None and limit is None:
            raise ValueError("If offset provided means the limit also must be provided")
        
        if offset is not None and limit is not None:
            if offset<1:
                offset=1
            offset=(offset-1)*limit

            query_stmt.offset(offset=offset).limit(limit=limit)

        elif limit is not None:
            query_stmt.limit(limit=limit)

        results=(
            await self.session.execute(
                query_stmt
            )
        ).mappings().all()

        return results
    

    async def getby_id(self,purchase_id:str,shop_id:str):
        query_stmt=(
            select(
                *self.purchase_cols
            )
            .where(
                Purchase.id==purchase_id,
                Purchase.shop_id==shop_id
            )
        )

        result=(await self.session.execute(query_stmt)).mappings().one_or_none()

        return result

    async def search(self, query, limit = 5):
        ...
        
