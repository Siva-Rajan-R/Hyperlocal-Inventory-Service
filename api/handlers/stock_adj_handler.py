from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from infras.primary_db.services.stock_adj_service import StockAdjService
from sqlalchemy.ext.asyncio import AsyncSession
from core.data_formats.enums.stock_adj_enums import StockAdjustmentMovementType,StockAdjustmentTypesEnum
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,ErrorResponseTypDict,BaseResponseTypDict
from schemas.v1.request_schemas.stock_adj_schema import CreateStockAdjSchema,GetStockAdjByShopIdSchema,GetStockAdjByIdSchema,GetAllStockAdjSchema,GetStockAdjByInventoryIdSchema
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


    async def create(self, data:CreateStockAdjSchema):
        modified_data=CreateStockAdjSchema(
            **data.model_dump(exclude=['movement_type']),movement_type=StockAdjustmentMovementType.STOCK_ADJUSTMENT
        )
        res=await self.stock_adj_service_obj.create_v2(data=modified_data)
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

    async def create_bulk(self,datas:List[CreateStockAdjSchema]):
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
    
    async def update(self,data:CreateStockAdjSchema):
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
    
        
    async def getby_shop_id(self,data:GetStockAdjByShopIdSchema):
        from infras.read_db.repos.stock_movement_repo import StockMovementReadDbRepo, StockMovementStatsReadDbRepo
        res=await StockMovementReadDbRepo.get_all_movements(data=data)
        stats_res=await StockMovementStatsReadDbRepo.get_stats(shop_id=data.shop_id)
        
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment fetched successfully",
                status_code=200,
                success=True
            ),
            data={
                "movements": res,
                "overall_stats": stats_res
            }
        )
    
    async def getby_inventory_id(self,data:GetStockAdjByInventoryIdSchema):
        from infras.read_db.repos.stock_movement_repo import StockMovementReadDbRepo
        res=await StockMovementReadDbRepo.get_movements_by_inventory_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )
    

    async def get(self,data:GetAllStockAdjSchema):
        from infras.read_db.repos.stock_movement_repo import StockMovementReadDbRepo, StockMovementStatsReadDbRepo
        res=await StockMovementReadDbRepo.get_all_movements(data=data)
        stats_res=await StockMovementStatsReadDbRepo.get_stats(shop_id=data.shop_id)
        
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment fetched successfully",
                status_code=200,
                success=True
            ),
            data={
                "movements": res,
                "overall_stats": stats_res
            }
        )
    
    async def getby_id(self,data:GetStockAdjByIdSchema):
        from infras.read_db.repos.stock_movement_repo import StockMovementReadDbRepo
        res=await StockMovementReadDbRepo.get_movement_by_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )

    async def search(self, shop_id: str, query: str, limit: int = 5):
        from infras.read_db.repos.stock_movement_repo import StockMovementReadDbRepo
        from schemas.v1.request_schemas.stock_adj_schema import GetStockAdjByShopIdSchema
        req_data = GetStockAdjByShopIdSchema(shop_id=shop_id, query=query, limit=limit, offset=1)
        res = await StockMovementReadDbRepo.get_all_movements(data=req_data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Stock Adjustment fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )


        