from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.inventory_model import StockAdjustments
from schemas.v1.db_schemas.stock_adj_schema import StockAdjCreateDbSchema,StockAdjUpdateDbSchema
from schemas.v1.request_schemas.stock_adj_schema import StockAdjCreateSchema,StockAdjUpdateSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from ..repos.stock_adj_repo import StockAdjRepo
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from ..services.inventory_service import InventoryService
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum


class StockAdjService(BaseServiceModel):
    def __init__(self, session:AsyncSession):
        self.stock_adj_repo_obj=StockAdjRepo(session=session)
        super().__init__(session)


    async def create(self, data:StockAdjCreateSchema):
        ic(data)
        stock_adj_id=generate_uuid()
        data_toadd=StockAdjCreateDbSchema(
           id=stock_adj_id,
           shop_id=data.datas.shop_id,
           datas=data.datas.model_dump(mode='json')
        )

        increament_qty={}
        decrement_qty={}
        ic(data.datas.products)
        for product in data.datas.products:
            ic(product)
            if product.type.value==StockAdjustmentTypesEnum.INCREMENT.value:
                increament_qty[product.barcode]=product.quantity
            elif product.type.value==StockAdjustmentTypesEnum.DECREMENT.value:
                decrement_qty[product.barcode]=product.quantity
            else:
                return False
        ic(increament_qty,decrement_qty) 
        if len(increament_qty)<1 and len(decrement_qty)<1:
            return False
        
        res=await self.stock_adj_repo_obj.create(data=data_toadd)
        ic(res)
        if res:
            ic(increament_qty)
            ic(decrement_qty)
            if increament_qty and len(increament_qty)>0:
                inv_incr_res=await InventoryService(session=self.session).update_qty_bulk(shop_id=data.datas.shop_id,data=increament_qty)
                ic(inv_incr_res)
            if decrement_qty and len(decrement_qty)>0:
                inv_decr_res=await InventoryService(session=self.session).update_qty_decr_bulk(shop_id=data.datas.shop_id,data=decrement_qty)
                ic(inv_decr_res)
            
        return res

    async def create_bulk(self,datas:List[StockAdjCreateSchema]):
        datas_toadd=[]
        for data in datas:
            StockAdjustments(id=generate_uuid(),**data.model_dump(mode='json'))

        res=await self.stock_adj_repo_obj.add_all(datas_toadd)
        return res
    
    async def update(self,data:StockAdjUpdateSchema):
        data_toupdate=StockAdjUpdateDbSchema(**data.model_dump(mode='json'))
        res=await self.stock_adj_repo_obj.update(data=data_toupdate)

        return res
        
    
    async def delete(self,stock_adj_id:str,shop_id:str):
        res=await self.stock_adj_repo_obj.delete(stock_adj_id=stock_adj_id,shop_id=shop_id)
        return res
    
    async def bulk_check(self,shop_id:str,stock_adj_ids:list):
        res=await self.stock_adj_repo_obj.bulk_check(shop_id=shop_id,stock_adj_ids=stock_adj_ids)
        return res
        
    async def get(self,timezone:TimeZoneEnum,shop_id:str,query:str="",limit:Optional[int]=None,offset:Optional[int]=None,full:Optional[bool]=True):
        res=await self.stock_adj_repo_obj.get(timezone=timezone,shop_id=shop_id,query=query,limit=limit,offset=offset,full=full)
        return res
    
    async def getby_id(self,timezone:TimeZoneEnum,stock_adj_id:str,shop_id:str):
        res=await self.stock_adj_repo_obj.getby_id(timezone=timezone,stock_adj_id=stock_adj_id,shop_id=shop_id)
        return res

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """
        


        