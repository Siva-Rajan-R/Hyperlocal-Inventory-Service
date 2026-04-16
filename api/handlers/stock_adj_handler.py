from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from infras.primary_db.services.stock_adj_service import StockAdjService
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,ErrorResponseTypDict,BaseResponseTypDict
from schemas.v1.request_schemas.stock_adj_schema import StockAdjCreateSchema,StockAdjUpdateSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from fastapi.exceptions import HTTPException


class HandleStockAdjRequest:
    def __init__(self, session:AsyncSession):
        self.stock_adj_service_obj=StockAdjService(session=session)
        self.session=session


    async def create(self, data:StockAdjCreateSchema):
        res=await self.stock_adj_service_obj.create(data=data)
        ic(res)
        if not res:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    status_code=400,
                    msg="Invalid datas for creating Stock Adjustments",
                    description="May be a mistmatch of the fields type",
                    success=False
                )
            )
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment created successfully",
                status_code=201,
                success=True
            )
        )

    async def create_bulk(self,datas:List[StockAdjCreateSchema]):
        res=await self.stock_adj_service_obj.create_bulk(datas)
        if not res:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    status_code=400,
                    msg="Invalid datas for creating bulk Stock Adjustments",
                    description="May be a mistmatch of the fields type",
                    success=False
                )
            )
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment Created-Bulk successfully",
                status_code=201,
                success=True
            )
        )
    
    async def update(self,data:StockAdjUpdateSchema):
        res=await self.stock_adj_service_obj.update(data=data)
        if not res:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    status_code=400,
                    msg="Invalid datas for updating Stock Adjustments",
                    description="May be a mistmatch of the fields type",
                    success=False
                )
            )
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment updating successfully",
                status_code=200,
                success=True
            )
        ) 
    
    async def delete(self,stock_adj_id:str,shop_id:str):
        res=await self.stock_adj_service_obj.delete(stock_adj_id=stock_adj_id,shop_id=shop_id)
        if not res:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    status_code=400,
                    msg="Invalid datas for deleting Stock Adjustments",
                    description="May be a mistmatch of the fields type",
                    success=False
                )
            )
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment deleted successfully",
                status_code=200,
                success=True
            )
        )
    
        
    async def get(self,timezone:TimeZoneEnum,shop_id:str,query:str="",limit:Optional[int]=None,offset:Optional[int]=None,full:Optional[bool]=True):
        res=await self.stock_adj_service_obj.get(timezone=timezone,shop_id=shop_id,query=query,limit=limit,offset=offset,full=full)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )
    
    async def getby_id(self,timezone:TimeZoneEnum,stock_adj_id:str,shop_id:str):
        res=await self.stock_adj_service_obj.getby_id(timezone=timezone,stock_adj_id=stock_adj_id,shop_id=shop_id)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """
        


        