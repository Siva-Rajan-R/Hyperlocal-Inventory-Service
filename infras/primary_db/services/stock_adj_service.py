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


    async def create(self, data:CreateStockAdjSchema,can_update_stock:Optional[bool]=True):
        ic(data)
        stockadj_id=generate_uuid()
        
        verified_inv_product=[]
        verified_variant=[]
        verified_batch=[]
        verified_serialno=[]

        checking_formatted_data={}

        for product in data.products:
            if (not product.batch_id and not product.variant_id) and product.inventory_id in verified_inv_product:
                ic("Same product should not be added twice")
                return False
            
            if product.variant_id and product.variant_id in verified_variant:
                ic("Same product + variant should not be added twice")
                return False
            
            if product.batch_id and product.batch_id in verified_batch:
                ic("Same product + batch should not be added twice")
                return False

            if product.serialno_id and product.serialno_id in verified_serialno:
                ic("Same product + serial number should not be added twice")
                return False

            if product.batch_id:
                verified_batch.append(product.batch_id)

            if product.variant_id:
                verified_variant.append(product.variant_id)

            if product.serialno_id:
                verified_serialno.append(product.serialno_id)
            
            if product.inventory_id not in verified_inv_product:
                verified_inv_product.append(product.inventory_id)

            if product.inventory_id not in checking_formatted_data:
                checking_formatted_data[product.inventory_id] = []

            checking_formatted_data[product.inventory_id].append(
                product.model_dump(mode="json")
            )

        ic(verified_inv_product, verified_variant, verified_batch, verified_serialno, checking_formatted_data)

        inv_repo_obj = InventoryRepo(session=self.session)
        inv_checked_results = await inv_repo_obj.bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=verified_inv_product))
        variant_checked_results = await inv_repo_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=verified_variant)
        batch_checked_results = await inv_repo_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=verified_batch)
        serialno_checked_results = await inv_repo_obj.bulk_serialno_check(shop_id=data.shop_id,serialnos_id=verified_serialno)

        structured_inventory = {}
        for result in inv_checked_results:
            structured_inventory[result['id']] = result

        structured_variant = {}
        for variant in variant_checked_results:
            structured_variant[variant['id']] = variant
        
        structured_batch = {}
        for batch in batch_checked_results:
            structured_batch[batch['id']] = batch

        structured_serialno = {}
        for serial in serialno_checked_results:
            structured_serialno[serial['id']] = serial

        ic(structured_inventory, structured_variant, structured_batch, structured_serialno)

        if len(verified_inv_product) != len(inv_checked_results) or \
           len(verified_variant) != len(variant_checked_results) or \
           len(verified_batch) != len(batch_checked_results) or \
           len(verified_serialno) != len(serialno_checked_results):
            ic("Some of the IDs are mismatching/not found in primary DB")
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

        ERROR_OCCURED=False
        for inv_res in inv_checked_results:
            inv_prod_id = inv_res['id']
            has_variant = inv_res['has_variant']
            has_batch = inv_res['has_batch']
            has_serialno = inv_res['has_serialno']
            
            # Net change for this base product
            net_inv_change = 0.0

            for requested_data in checking_formatted_data[inv_prod_id]:
                batch_id:str=requested_data.get('batch_id',None)
                variant_id:str=requested_data.get('variant_id',None)
                serial_id:str=requested_data.get("serialno_id",None)
                stocks:float=requested_data['stocks']
                adjustment_type=requested_data['type']
                serial_numbers = requested_data.get('serial_numbers', []) or []

                ic(batch_id, inv_prod_id, variant_id, serial_id, stocks, adjustment_type)

                if has_variant and not variant_id:
                    ic("There is no variant id")
                    ERROR_OCCURED=True
                    return None
                
                if has_batch and not batch_id:
                    ic("There is no batch id")
                    ERROR_OCCURED=True
                    return None

                if has_serialno and not serial_id:
                    ic("Serial number ID is missing for serialized inventory")
                    ERROR_OCCURED=True
                    return None

                if has_serialno and len(serial_numbers) != stocks:
                    ic("Invalid Serial numbers length vs stocks count")
                    ERROR_OCCURED=True
                    return None

                # Determine the correct stocks_before
                stocks_before = inv_res['stocks']
                if has_variant and variant_id in structured_variant:
                    stocks_before = structured_variant[variant_id]['stocks']
                if has_batch and batch_id in structured_batch:
                    stocks_before = structured_batch[batch_id]['stocks']

                # Track quantities for updates
                is_increment = (adjustment_type == StockAdjustmentTypesEnum.INCREMENT.value or adjustment_type == StockAdjustmentTypesEnum.INCREMENT)
                is_decrement = (adjustment_type == StockAdjustmentTypesEnum.DECREMENT.value or adjustment_type == StockAdjustmentTypesEnum.DECREMENT)

                if is_increment:
                    if has_variant and variant_id:
                        variant_toincr[variant_id]=stocks
                    if has_batch and batch_id:
                        batch_toincr[batch_id]=stocks
                    if has_serialno and serial_id:
                        serailno_incr[serial_id]=serial_numbers
                    net_inv_change += stocks

                elif is_decrement:
                    if has_variant and variant_id:
                        variant_todecr[variant_id]=stocks
                    if has_batch and batch_id:
                        batch_todecr[batch_id]=stocks
                    if has_serialno and serial_id:
                        serailno_decr[serial_id]=serial_numbers
                    net_inv_change -= stocks

                stock_adj_inv_prod_toadd.append(
                    StockAdjustmentInventoryProducts(
                        inventory_id=inv_prod_id,
                        stockadjustment_id=stockadj_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        stocks=stocks,
                        type=adjustment_type,
                        stocks_before=stocks_before
                    )
                )

            # Apply net inventory change
            if net_inv_change > 0:
                inventory_toincr[inv_prod_id] = net_inv_change
            elif net_inv_change < 0:
                inventory_todecr[inv_prod_id] = abs(net_inv_change)

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
            movement_type=data.movement_type,
            description=data.description,
            datas=data.datas
        )

        stockadj_res=await stockadj_repo_obj.create(data=stock_adjtoadd)
        ic(stockadj_res)
        NEXT=stockadj_res
        ic(NEXT)
        if NEXT:
            stockadj_inv_prod_res=await stockadj_repo_obj.create_bulk_stockadj_inv_prod(datas=stock_adj_inv_prod_toadd)
            NEXT=stockadj_inv_prod_res
        ic(NEXT)
        if NEXT and can_update_stock:
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

        


        