from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.inventory_model import StockAdjustments
from schemas.v1.db_schemas.stock_adj_schema import StockAdjCreateDbSchema,StockAdjUpdateDbSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic


class StockAdjRepo(BaseRepoModel):
    def __init__(self, session:AsyncSession):
        self.stock_adj_cols=(
            StockAdjustments.id,
            StockAdjustments.shop_id,
            StockAdjustments.datas
        )

        super().__init__(session)

    @start_db_transaction
    async def create(self, data:StockAdjCreateDbSchema):
        ic(data)
        data_toadd=StockAdjustments(**data.model_dump(mode="json"))

        res=self.session.add(data_toadd)
        ic(res)
        return data
    
    @start_db_transaction
    async def create_bulk(self,datas:List[StockAdjustments]):
        res=self.session.add_all(datas)
        return True
    
    @start_db_transaction
    async def update(self,data:StockAdjUpdateDbSchema):
        stock_adj_data=data.model_dump(mode="json",exclude_unset=True)
        
        ic("inside nn")
        stock_adj_toupdate=(
                update(StockAdjustments)
                .where(
                    StockAdjustments.id==data.id,
                    StockAdjustments.shop_id==data.shop_id
                )
                .values(datas=data.datas)
        ).returning(StockAdjustments.id)
        is_updated=(await self.session.execute(stock_adj_toupdate)).scalar_one_or_none()

        if not is_updated:
            return False

        return True
        
    
    @start_db_transaction
    async def delete(self,stock_adj_id:str,shop_id:str):
        stock_adj_del=(
            delete(StockAdjustments)
            .where(
                StockAdjustments.id==stock_adj_id,
                StockAdjustments.shop_id==shop_id
            )
        ).returning(StockAdjustments.id)

        is_deleted=(await self.session.execute(stock_adj_del)).scalar_one_or_none()

        return is_deleted
    
    async def bulk_check(self,shop_id:str,stock_adj_ids:list):
        check_stmt=(
            select(
                StockAdjustments.id
            )
            .where(
                StockAdjustments.id.in_(stock_adj_ids),
                StockAdjustments.shop_id==shop_id
            )
        )

        results=(await self.session.execute(check_stmt)).scalars().all()

        ic(results)

        return results
        
    async def get(self,timezone:TimeZoneEnum,shop_id:str,query:str="",limit:Optional[int]=None,offset:Optional[int]=None,full:Optional[bool]=True):
        created_at=func.date(func.timezone(timezone.value,StockAdjustments.created_at))
        ic(shop_id,query)
        select_stmt=(
            select(*self.stock_adj_cols,created_at)
            .where(
                StockAdjustments.shop_id==shop_id
            )
        )

        results=(
            await self.session.execute(
                select_stmt
            )
        ).mappings().all()

        ic(results)
        if offset is not None and limit is None:
            raise ValueError("If offset provided means the limit also must be provided")
        
        if offset is not None and limit is not None:
            if offset<1:
                offset=1
            offset=(offset-1)*limit

            select_stmt.offset(offset=offset).limit(limit=limit)

        elif limit is not None:
            select_stmt.limit(limit=limit)

        ic("Hello")
        results=(
            await self.session.execute(
                select_stmt
            )
        ).mappings().all()

        ic("jeeva",results)
        
        if not full and len(results)==1:
            results=results[0]

        return results
    
    async def getby_id(self,timezone:TimeZoneEnum,stock_adj_id:str,shop_id:str):
        created_at=func.date(func.timezone(timezone.value,StockAdjustments.created_at))
        stmt=(
            select(
                *self.stock_adj_cols,
                created_at
            )
            .where(
                StockAdjustments.id==stock_adj_id,
                StockAdjustments.shop_id==shop_id
            )
        )

        results=(await self.session.execute(stmt)).mappings().one_or_none()

        return results

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """
        


        