from models.service_models.base_service_model import BaseServiceModel
from ..repos.inventory_repo import InventoryRepo
from typing import Optional,List
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from schemas.v1.db_schemas.inventory_schema import InventoryProductCategoryEnum,AddInventoryDbSchema,UpdateInventoryDbSchema,UpdateVarientProductDbSchema
from schemas.v1.request_schemas.inventory_schema import AddInventorySchema,UpdateInventorySchema
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from icecream import ic
from ..models.inventory_model import Inventory,InventoryVariants,InventoryBathces,StockAdjustments
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction

class InventoryService(BaseServiceModel):
    
    async def create(self,data:AddInventorySchema,added_by:str,product_data:dict):
        """
        Need to check shop existence and product data, then added by
        """
        # just check the product existss or not then adding 
        # cheecing the serial numbers and also if it has a varients and athe varient have a serieal number means checking
        req_prod_name,req_prod_desc,req_prod_category=data.datas.name.lower(),data.datas.description.lower(),data.datas.category.lower()
        prod_name,prod_desc,prod_category=product_data.get('name'),product_data.get('description'),product_data.get('category')

        if not prod_name or not prod_desc or not prod_category:
            return False
        
        ic(data.model_dump())
        inventory_id:str=generate_uuid()

        datas=data.datas.model_dump(mode='json')
        ic(datas)
        # vartiantss checking process
        variants_toadd=[]
        if data.datas.has_varients:
            for variant in datas['varients']:
                if (data.datas.has_serialno_tracking and len(variant['serial_numbers'])!=variant['stocks']):
                    return False

                variant_id=generate_uuid()
                variant_datas=variant.copy()
                del variant_datas['batches']
                variants_toadd.append(
                    InventoryVariants(
                        id=variant_id,
                        shop_id=data.datas.shop_id,
                        inventory_id=inventory_id,
                        stocks=variant['stocks'],
                        buy_price=variant['buy_price'],
                        sell_price=variant['sell_price'],
                        barcode=variant['barcode'],
                        datas=variant_datas
                    )
                )
        del datas['varients']
        ic(datas)
        # if no variants but serial tracking means checks
        if not data.datas.has_varients and data.datas.has_serialno_tracking:
            if len(data.datas.serial_numbers)!=data.datas.stocks:
                return False
            

        data=AddInventoryDbSchema(
            datas=datas,
            shop_id=data.datas.shop_id,
            barcode=data.datas.barcode,
            stocks=data.datas.stocks,
            buy_price=data.datas.buy_price,
            sell_price=data.datas.sell_price,
            id=inventory_id,
            added_by=added_by
        )

        inventory_res=await InventoryRepo(session=self.session).create(data=data)
        variant_res=None
        if variants_toadd and inventory_res:
            variant_res=await self.session.add_all(variants_toadd)
        if inventory_res or variant_res:
            return True
        
        return False
    

    async def create_bulk(self,datas:List[AddInventorySchema],added_by:str):
        datas_toadd=[]
        for data in datas:
            datas_toadd.append(
                Inventory(**data.model_dump(mode="json"),id=generate_uuid(),added_by=added_by)
            )
        
        return await InventoryRepo(session=self.session).create_bulk(datas=datas_toadd)
    

    async def update(self,data:UpdateInventorySchema,product_data:dict):
        req_prod_infos=[data.datas.name,data.datas.description,data.datas.category]
        prod_infos=[product_data.get('name'),product_data.get('description'),product_data.get('category')]
        if len(req_prod_infos)!=len(prod_infos):
            return False

        datas=data.datas.model_dump(mode='json')
        ic(datas)
        variants_toadd=[]

        if data.datas.has_varients:
            for variant in datas['varients']:
                variant_id=generate_uuid() if not variant.get('id') else variant['id']
                variant_datas=variant.copy()
                variants_toadd.append(
                    UpdateVarientProductDbSchema(
                        id=variant_id,
                        shop_id=data.datas.shop_id,
                        inventory_id=datas['id'],
                        buy_price=variant['buy_price'],
                        sell_price=variant['sell_price'],
                        datas=variant_datas,
                        stocks=variant['stocks'],
                        barcode=variant['barcode']
                    )
                )

        del datas['varients']
        ic(datas)
        ic(variants_toadd)

        data=UpdateInventoryDbSchema(
            id=data.datas.id,
            shop_id=data.datas.shop_id,
            barcode=data.datas.barcode,
            buy_price=data.datas.buy_price,
            sell_price=data.datas.sell_price,
            datas=datas

        )
        
        ic(data)
        inventory_res=await InventoryRepo(session=self.session).update(data=data)
        variant_res=await self.update_bulk_variants(datas=variants_toadd)
        if inventory_res and variant_res:
            return True
        return False
    
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
 
    async def delete(self,inventory_id:str,shop_id:str):
        return await InventoryRepo(session=self.session).delete(inventory_id=inventory_id,shop_id=shop_id)
    
    async def get(self,shop_id:str,timezone:TimeZoneEnum,offset:int,query:str="",limit:Optional[int]=10):
        return await InventoryRepo(session=self.session).get(
            timezone=timezone,
            query=query,
            limit=limit,
            offset=offset,
            shop_id=shop_id
        )
    
    async def getby_id(self,inventory_barcode_id:str,shop_id:str,timezone:TimeZoneEnum):
        return await InventoryRepo(session=self.session).get(
            timezone=timezone,
            shop_id=shop_id,
            query=inventory_barcode_id,
            full=False
        )
    
    async def bulk_check(self,barcodes:List[str],shop_id:str,additional_conditions: Optional[tuple]=()):
        return await InventoryRepo(session=self.session).bulk_check(barcodes=barcodes,shop_id=shop_id)
    
    async def bulk_varient_check(self,shop_id:str,variants_id:list,additional_conditions: Optional[tuple]=()):
        return await InventoryRepo(session=self.session).bulk_varient_check(shop_id=shop_id,variants_id=variants_id,additional_conditions=additional_conditions)
    
    
    async def search(self, query, limit = 5):
        """
        This is just a wrapper for baseservice-model
        Instead you can directly use a get method by adjusting the limit
        """
        ...