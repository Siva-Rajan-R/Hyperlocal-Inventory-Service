from models.service_models.base_service_model import BaseServiceModel
from ..repos.inventory_repo import InventoryRepo
from typing import Optional,List
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from schemas.v1.db_schemas.inventory_schema import InventoryProductCategoryEnum,AddInventoryDbSchema,UpdateInventoryDbSchema
from schemas.v1.request_schemas.inventory_schema import AddInventorySchema,UpdateInventorySchema
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from icecream import ic
from ..models.inventory_model import Inventory
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction

class InventoryService(BaseServiceModel):
    
    async def create(self,data:AddInventorySchema,added_by:str,product_data:dict):
        """
        Need to check shop existence and product data, then added by
        """
        req_prod_name,req_prod_desc,req_prod_category=data.product_name.lower(),data.product_description.lower(),data.product_category.lower()
        prod_name,prod_desc,prod_category=product_data.get('name'),product_data.get('description'),product_data.get('category')

        if not prod_name or not prod_desc or not prod_category:
            return False
        
        if req_prod_name==prod_name.lower() and req_prod_desc==prod_desc.lower() and req_prod_category==prod_category.lower():
            data.product_name=None
            data.product_description=None
            data.product_category=None
        ic(data.model_dump())
        inventory_id:str=generate_uuid()
        data=AddInventoryDbSchema(**data.model_dump(mode='json'),id=inventory_id,added_by=added_by)
        return await InventoryRepo(session=self.session).create(data=data)
    

    async def create_bulk(self,datas:List[AddInventorySchema],added_by:str):
        datas_toadd=[]
        for data in datas:
            datas_toadd.append(
                Inventory(**data.model_dump(mode="json"),id=generate_uuid(),added_by=added_by)
            )
        
        return await InventoryRepo(session=self.session).create_bulk(datas=datas_toadd)
    

    async def update(self,data:UpdateInventorySchema,product_data:dict):
        req_prod_infos=[data.product_name,data.product_description,data.product_category]
        prod_infos=[product_data.get('name'),product_data.get('description'),product_data.get('category')]
        prod_toadd={
            'product_name':None,
            'product_description':None,
            'product_category':None
        }

        ic("Hi Hell01")
        if len(req_prod_infos)!=len(prod_infos):
            return False
        ic("Hi Hell02")
        
        # first check, to find out what are the fields are need to change
        for req_p,prod,key in zip(req_prod_infos,prod_infos,['product_name','product_description','product_category']):
            if req_p!=prod:
                prod_toadd[key]=req_p
        
        # second check, to refill the remaining fields, if its all or not none 
        if prod_toadd['product_category'] or prod_toadd['product_description'] or prod_toadd['product_name']:
            for prod,key in zip(prod_infos,['product_name','product_description','product_category']):
                if prod_toadd[key] is None:
                    prod_toadd[key]=prod


        ic(f"finalized product data to add is => {prod_toadd}")
        data.product_name=prod_toadd['product_name']
        data.product_description=prod_toadd['product_description']
        data.product_category=prod_toadd['product_category']

        data=UpdateInventoryDbSchema(
            **data.model_dump(mode='json',exclude_unset=True,exclude_none=True)
        )
        
        ic(data)
        return await InventoryRepo(session=self.session).update(data=data)
    
    async def update_qty(self,barcode_inven_id:str,qty:int,shop_id:str):
        return await InventoryRepo(session=self.session).update_qty(barcode_inv_id=barcode_inven_id,shop_id=shop_id,qty=qty)
    
    async def update_qty_bulk(self,shop_id:str,data:dict):
        """
        Docstring for update_qty_bulk
        THe data contains product barcode as a key & the qty to increment as a value
        """
        return await InventoryRepo(session=self.session).bulk_qty_update(data=data,shop_id=shop_id)
    

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
    
    
    async def search(self, query, limit = 5):
        """
        This is just a wrapper for baseservice-model
        Instead you can directly use a get method by adjusting the limit
        """
        ...