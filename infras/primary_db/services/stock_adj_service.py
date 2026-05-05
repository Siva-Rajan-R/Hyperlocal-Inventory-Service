from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.inventory_model import StockAdjustments,StockAdjustmentInventoryProducts
from schemas.v1.db_schemas.stock_adj_schema import CreateStockAdjDbSchema
from schemas.v1.request_schemas.stock_adj_schema import CreateStockAdjSchema,GetStockAdjByShopIdSchema,GetAllStockAdjSchema,GetStockAdjByIdSchema,GetStockAdjByInventoryIdSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from ..repos.stock_adj_repo import StockAdjRepo
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from ..services.inventory_service import InventoryService
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum
from infras.primary_db.repos.inventory_repo import InventoryRepo,BulkCheckInventorySchema


class StockAdjService(BaseServiceModel):
    def __init__(self, session:AsyncSession):
        self.stock_adj_repo_obj=StockAdjRepo(session=session)
        super().__init__(session)


    async def create(self, data:CreateStockAdjSchema):
        ic(data)
        stockadj_id=generate_uuid()
        inventories_tocheck=[]
        formatted_req_inventories={}

        for inventory in data.products:
            inventories_tocheck.append(inventory.inventory_id)
            if formatted_req_inventories.get(inventory.inventory_id,None):
                return False
            
            formatted_req_inventories[inventory.inventory_id]=inventory.model_dump(mode="json")

        ic(inventories_tocheck,formatted_req_inventories)
        if len(inventories_tocheck)!=len(data.products):
            ic("Invalid datas length")
            return False
        
        checked_results=await InventoryRepo(session=self.session).bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inventories_tocheck))

        ic(checked_results)

        if len(checked_results)!=len(data.products):
            ic("Invalid checked results")
            return False
        
        variant_toincr={}
        batch_toincr={}
        inventory_toincr={}
        serailno_incr={}

        variant_todecr={}
        batch_todecr={}
        inventory_todecr={}
        serailno_decr={}

        stock_adj_inv_prod_toadd=[]

        ERROR_OCCURED=None
        for inv_res in checked_results:
            ic(inv_res)
            requested_data=formatted_req_inventories.get(inv_res['id'],None)
            ic(requested_data)

            batch_id:str=requested_data.get('batch_id',None)
            inv_id:str=requested_data.get('inventory_id',None)
            variant_id:str=requested_data.get('variant_id',None)
            serial_id:list=requested_data.get("serialno_id",None)
            stocks:int=requested_data['stocks']

            adjustment_type=requested_data['type']

            ic(batch_id,inv_id,variant_id,serial_id,stocks,adjustment_type)

            if inv_res['has_variant'] and not variant_id:
                    ic("There is no variant id")
                    ERROR_OCCURED=True
                    return None
                
            if inv_res['has_batch'] and not batch_id:
                ic("There is no batch id")
                ERROR_OCCURED=True
                return None
            

            if (inv_res['has_serialno'] and not serial_id) or ((inv_res['has_serialno'] and serial_id) and len(requested_data.get('serial_numbers',[]) or [])!=stocks):
                ic("invalid Serial numbers")
                ERROR_OCCURED=True
                return None
            

            if adjustment_type==StockAdjustmentTypesEnum.INCREMENT.value:
                if inv_res['has_variant'] and variant_id:
                    variant_toincr[variant_id]=stocks

                if inv_res['has_batch'] and batch_id:
                    batch_toincr[batch_id]=stocks

                if inv_res['has_serialno'] and serial_id:
                    serailno_incr[serial_id]=requested_data.get('serial_numbers')

                inventory_toincr[inv_id]=stocks

            if adjustment_type==StockAdjustmentTypesEnum.DECREMENT.value:
                if inv_res['has_variant'] and variant_id:
                    variant_todecr[variant_id]=stocks

                if inv_res['has_batch'] and batch_id:
                    batch_todecr[batch_id]=stocks

                if inv_res['has_serialno'] and serial_id:
                    serailno_decr[serial_id]=requested_data.get('serial_numbers')

                inventory_todecr[inv_id]=stocks
            

            stock_adj_inv_prod_toadd.append(
                StockAdjustmentInventoryProducts(
                    inventory_id=inv_id,
                    stockadjustment_id=stockadj_id,
                    variant_id=variant_id,
                    batch_id=batch_id,
                    stocks=stocks,
                    type=adjustment_type

                )
            )

        ic(inventory_toincr,variant_toincr,batch_toincr,serailno_incr,stock_adj_inv_prod_toadd)
        ic(inventory_todecr,variant_todecr,batch_todecr,serailno_decr)
        if ERROR_OCCURED:
            ic("Error occured")
            return False
        

        NEXT=False
        stockadj_repo_obj=StockAdjRepo(session=self.session)
        stock_adjtoadd=CreateStockAdjDbSchema(
            id=stockadj_id,
            shop_id=data.shop_id,
            adjusted_date=data.adjusted_date,
            description=data.description,
            datas=data.datas
        )

        stockadj_res=await stockadj_repo_obj.create(data=stock_adjtoadd)
        NEXT=stockadj_res

        if NEXT:
            stockadj_inv_prod_res=await stockadj_repo_obj.create_bulk_stockadj_inv_prod(datas=stock_adj_inv_prod_toadd)
            NEXT=stockadj_inv_prod_res

        if NEXT:
            inv_repo_obj=InventoryRepo(session=self.session)
            if inventory_toincr:
                await inv_repo_obj.bulk_qty_update(data=inventory_toincr,shop_id=data.shop_id)
            if variant_toincr:
                await inv_repo_obj.bulk_variant_qty_update(data=variant_toincr,shop_id=data.shop_id)
            if batch_toincr:
                await inv_repo_obj.bulk_batch_qty_update(data=batch_toincr,shop_id=data.shop_id)
            if serailno_incr:
                await inv_repo_obj.bulk_add_serialno(data=serailno_incr,shop_id=data.shop_id)

            if inventory_todecr:
                await inv_repo_obj.bulk_qty_decr_update(data=inventory_todecr,shop_id=data.shop_id)
            if variant_todecr:
                await inv_repo_obj.bulk_variant_decr_qty_update(data=variant_todecr,shop_id=data.shop_id)
            if batch_todecr:
                await inv_repo_obj.bulk_batch_decr_qty_update(data=batch_todecr,shop_id=data.shop_id)
            if serailno_decr:
                ic("Need to implement")
                await inv_repo_obj.bulk_update_serialno(data=serailno_decr,shop_id=data.shop_id)

        return NEXT

            


            

        

        

    async def create_bulk(self,datas:List[CreateStockAdjSchema]):
        datas_toadd=[]
        for data in datas:
            StockAdjustments(id=generate_uuid(),**data.model_dump(mode='json'))

        res=await self.stock_adj_repo_obj.add_all(datas_toadd)
        return res
    
    async def update(self,data:CreateStockAdjSchema):
        data_toupdate=StockAdjUpdateDbSchema(**data.model_dump(mode='json'))
        res=await self.stock_adj_repo_obj.update(data=data_toupdate)

        return res
        
    
    async def delete(self,stock_adj_id:str,shop_id:str):
        res=await self.stock_adj_repo_obj.delete(stock_adj_id=stock_adj_id,shop_id=shop_id)
        return res
    
    async def bulk_check(self,shop_id:str,stock_adj_ids:list):
        res=await self.stock_adj_repo_obj.bulk_check(shop_id=shop_id,stock_adj_ids=stock_adj_ids)
        return res
        
    async def get(self,data:GetAllStockAdjSchema):
        res=await self.stock_adj_repo_obj.get(data=data)
        return res
    
    async def getby_shop_id(self,data:GetStockAdjByShopIdSchema):
        res=await self.stock_adj_repo_obj.getby_shop_id(data=data)
        return res
    
    async def getby_id(self,data:GetStockAdjByIdSchema):
        res=await self.stock_adj_repo_obj.getby_id(data=data)
        return res
    
    async def getby_inventory_id(self,data:GetStockAdjByInventoryIdSchema):
        res=await self.stock_adj_repo_obj.getby_inventory_id(data=data)
        return res

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """

        


        