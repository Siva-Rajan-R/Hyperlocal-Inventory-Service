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
from infras.primary_db.repos.inventory_repo import InventoryRepo


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

        inv_increament_qty={}
        inv_decrement_qty={}

        varient_increament_qty={}
        varient_decrement_qty={}

        batch_increament_qty={}
        batch_decrement_qty={}

        inv_serialno_increaments={}
        inv_serialno_decrements={}

        variant_serialno_increaments={}
        variant_serialno_decrements={}

        ic(data.datas.products)
        for product in data.datas.products:
            ic(product)
            if product.type.value==StockAdjustmentTypesEnum.INCREMENT.value:
                if product.varient_id:
                    varient_increament_qty[product.varient_id]=product.quantity
                    if product.serial_numbers and len(product.serial_numbers)>0:
                        variant_serialno_increaments[product.varient_id]=product.serial_numbers

                if product.batch_id:
                    batch_increament_qty[product.batch_id]=product.quantity

                if not product.varient_id:
                    inv_increament_qty[product.barcode]=product.quantity
                    if product.serial_numbers and len(product.serial_numbers)>0:
                        inv_serialno_increaments[product.barcode]=product.serial_numbers

            elif product.type.value==StockAdjustmentTypesEnum.DECREMENT.value:
                if product.varient_id:
                    varient_decrement_qty[product.varient_id]=product.quantity
                    if product.serial_numbers and len(product.serial_numbers)>0:
                        variant_serialno_decrements[product.varient_id]=product.serial_numbers
                if product.batch_id:
                    batch_decrement_qty[product.batch_id]=product.quantity
                if not product.varient_id:
                    inv_decrement_qty[product.barcode]=product.quantity
                    if product.serial_numbers and len(product.serial_numbers)>0:
                        inv_serialno_decrements[product.barcode]=product.serial_numbers
            else:
                return False
        ic(inv_increament_qty,inv_decrement_qty,varient_increament_qty,varient_decrement_qty,batch_increament_qty,batch_decrement_qty,inv_serialno_increaments,inv_serialno_decrements,variant_serialno_increaments,variant_serialno_decrements)
        
        res=await self.stock_adj_repo_obj.create(data=data_toadd)
        ic(res)
        if res:
            ic(inv_increament_qty)
            ic(inv_decrement_qty)
            # increament
            # invntory
            inv_incr_res=await InventoryService(session=self.session).update_qty_bulk(shop_id=data.datas.shop_id,data=inv_increament_qty)
            inv_serialno_incr_res=await InventoryRepo(session=self.session).bulk_serialnumber_update(shop_id=data.datas.shop_id,data=inv_serialno_increaments)
            # batches
            batch_incr_res=await InventoryRepo(session=self.session).bulk_batch_qty_update(shop_id=data.datas.shop_id,data=batch_increament_qty)
            # varients
            varient_incr_res=await InventoryRepo(session=self.session).bulk_variant_qty_update(shop_id=data.datas.shop_id,data=varient_increament_qty)
            varient_serialno_incr_res=await InventoryRepo(session=self.session).bulk_variant_serialnumber_update(shop_id=data.datas.shop_id,data=variant_serialno_increaments)
            ic(inv_incr_res,varient_incr_res,batch_incr_res,inv_serialno_incr_res,varient_serialno_incr_res)

            # Decremamnt
            # inventory
            inv_decr_res=await InventoryService(session=self.session).update_qty_decr_bulk(shop_id=data.datas.shop_id,data=inv_decrement_qty)
            inv_serialno_decr_res=await InventoryRepo(session=self.session).bulk_serialnumber_remove(shop_id=data.datas.shop_id,data=inv_serialno_decrements)
            # batches
            batch_decr_res=await InventoryRepo(session=self.session).bulk_batch_decr_qty_update(shop_id=data.datas.shop_id,data=batch_decrement_qty)
            # varients
            varient_decr_res=await InventoryRepo(session=self.session).bulk_variant_decr_qty_update(shop_id=data.datas.shop_id,data=varient_decrement_qty)
            varient_serialno_decr_res=await InventoryRepo(session=self.session).bulk_variant_serialnumber_remove(shop_id=data.datas.shop_id,data=variant_serialno_decrements)
            ic(inv_decr_res,varient_decr_res,batch_decr_res,inv_serialno_decr_res,varient_serialno_decr_res)
            
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
        


        