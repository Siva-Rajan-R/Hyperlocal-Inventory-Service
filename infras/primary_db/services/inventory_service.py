from models.service_models.base_service_model import BaseServiceModel
from ..models.inventory_model import Inventory,InventoryBatches,InventorySerialNumbers,InventoryVariants
from ..repos.inventory_repo import InventoryRepo
from typing import Optional,List
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from schemas.v1.db_schemas.inventory_schema import InventoryProductCategoryEnum,CreateInventoryDbSchema,UpdateInventoryDbSchema,UpdateVarientProductDbSchema,InventoryBatchDbSchema,InventoryVariantDbSchema,InventorySerialNumberDbSchema
from schemas.v1.request_schemas.inventory_schema import CreateInventorySchema,UpdateInventorySchema,DeleteInventorySchema,GetAllInventorySchema,GetInventoryByIdSchema,GetInventoryByShopIdSchema,VerifySchema,BulkCheckInventorySchema
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from icecream import ic
from ..models.inventory_model import Inventory,InventoryVariants,InventoryBatches,StockAdjustments
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction

class InventoryService(BaseServiceModel):
    
    async def create(self,data:CreateInventorySchema):
        """
        Need to check shop existence and product data, then added by
        """

        is_exists=await self.verify(data=VerifySchema(barcode=data.barcode,shop_id=data.shop_id))
        ic(is_exists)
        if is_exists['exists']:
            ic("Product already exist with the barcode")
            return False
        
        inventory_id:str=generate_uuid()
        ic(inventory_id)
        variants_toadd=[]
        ERROR_OCCURED:bool=False

        if data.has_variant:
            for variant in data.variants:
                variant_id:str=generate_uuid()
                variants_toadd.append(
                    InventoryVariants(
                        id=variant_id,
                        shop_id=data.shop_id,
                        inventory_id=inventory_id,
                        sell_price=variant.sell_price,
                        name=variant.name,
                        buy_price=variant.buy_price,
                        stocks=variant.stocks,
                        datas=variant.datas
                    )
                )
            
        inventorydata_toadd=data.model_dump()
        inventorydata_toadd['stocks']=0
        # Create a better is for the sku and it to be unique
        inventorydata_toadd['sku']=generate_uuid()

        next=True
        inv_repo_obj=InventoryRepo(session=self.session)

        inv_res=await inv_repo_obj.create(data=CreateInventoryDbSchema(**inventorydata_toadd,id=inventory_id,is_active=False))
        next=inv_res
        if next and variants_toadd:
            variant_res=await inv_repo_obj.create_variant_bulk(datas=variants_toadd)
            ic(variant_res)
            next=variant_res
        
        return next

 

    async def create_bulk(self,datas:List[CreateInventorySchema],added_by:str):
        datas_toadd=[]
        for data in datas:
            datas_toadd.append(
                Inventory(**data.model_dump(mode="json"),id=generate_uuid(),added_by=added_by)
            )
        
        return await InventoryRepo(session=self.session).create_bulk(datas=datas_toadd)
    


    async def update(self,data:UpdateInventorySchema):
        res=await InventoryRepo(session=self.session).update(
            data=UpdateInventoryDbSchema(
                **data.model_dump(mode='json',exclude_none=True,exclude_unset=True)
            )
        )

        return res
        

    async def update_qty(self,barcode_inven_id:str,qty:int,shop_id:str):
        return await InventoryRepo(session=self.session).update_qty(barcode_inv_id=barcode_inven_id,shop_id=shop_id,qty=qty)
    
    async def update_qty_bulk(self,shop_id:str,data:dict):
        """
        Docstring for update_qty_bulk
        THe data contains product barcode as a key & the qty to increment as a value
        """
        return await InventoryRepo(session=self.session).bulk_qty_update(data=data,shop_id=shop_id)
    

    async def bulk_serialnumber_update(self, data: dict, shop_id: str):
        """
        data = {
            barcode: [serial_numbers]
        }
        """
        return await InventoryRepo(session=self.session).bulk_serialnumber_update(data=data,shop_id=shop_id)
    
    async def update_bulk_variants(self,datas: List[UpdateVarientProductDbSchema]):
        return await InventoryRepo(session=self.session).bulk_variant_update(datas=datas)
    
    async def update_sellprice_bulk(self,shop_id:str,data:dict):
        """
        Docstring for update_qty_bulk
        THe data contains product barcode as a key & the qty to increment as a value
        """
        return await InventoryRepo(session=self.session).bulk_sellprice_update(data=data,shop_id=shop_id)
    
    async def update_buyprice_bulk(self,shop_id:str,data:dict):
        """
        Docstring for update_qty_bulk
        THe data contains product barcode as a key & the qty to increment as a value
        """
        return await InventoryRepo(session=self.session).bulk_buyprice_update(data=data,shop_id=shop_id)
    

    async def update_qty_decr_bulk(self,shop_id:str,data:dict):
        """
        Docstring for update_qty_bulk
        THe data contains product barcode as a key & the qty to increment as a value
        """
        return await InventoryRepo(session=self.session).bulk_qty_decr_update(data=data,shop_id=shop_id)
 
    async def delete(self,data:DeleteInventorySchema):
        return await InventoryRepo(session=self.session).delete(data=data)
    
    async def get(self,data:GetAllInventorySchema):
        return await InventoryRepo(session=self.session).get(data=data)
    

    async def getby_shop_id(self,data:GetInventoryByShopIdSchema):
        return await InventoryRepo(session=self.session).getby_shop_id(data=data)
    
    async def getby_id(self,data:GetInventoryByIdSchema):
        return await InventoryRepo(session=self.session).getby_id(data=data)
    
    async def bulk_check(self,data:BulkCheckInventorySchema)-> List[dict] | list:
        return await InventoryRepo(session=self.session).bulk_check(data=data)
    
    async def bulk_varient_check(self,shop_id:str,variants_id:list,additional_conditions: Optional[tuple]=()):
        return await InventoryRepo(session=self.session).bulk_varient_check(shop_id=shop_id,variants_id=variants_id,additional_conditions=additional_conditions)
    
    
    async def search(self, query, limit = 5):
        """
        This is just a wrapper for baseservice-model
        Instead you can directly use a get method by adjusting the limit
        """
        
        ...

    async def verify(self,data:VerifySchema):
        data_tocheck=data.model_dump(mode='json',exclude=['shop_id'],exclude_none=True,exclude_unset=True)

        if not data_tocheck or len(data_tocheck)<1:
            return False

        res=await InventoryRepo(session=self.session).verify(data=data)

        return res