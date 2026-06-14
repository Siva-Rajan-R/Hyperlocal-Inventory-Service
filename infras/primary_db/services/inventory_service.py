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
import httpx

ACTIVITY_LOG_URL = "http://127.0.0.1:8001/activity-logs"

async def _send_activity_log(shop_id: str, action: str, entity_id: str, description: str, changes: list = None):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(ACTIVITY_LOG_URL, json={
                "shop_id": shop_id,
                "user_name": "siva",
                "service": "Inventory",
                "action": action,
                "entity_type": "Product",
                "entity_id": entity_id,
                "description": description,
                "changes": changes or []
            })
    except Exception as e:
        ic(f"Failed to log activity: {e}")

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

        if data.has_variant and not data.variants:
            ic("Invalid variants, Variant enamble but no variants was created")
            return False
        
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
                        datas=variant.datas,
                        reorder_point=variant.reorder_point,
                        sku=generate_uuid()
                    )
                )
            
        inventorydata_toadd=data.model_dump()
        inventorydata_toadd['stocks']=0
        # Create a better is for the sku and it to be unique
        inventorydata_toadd['sku']=generate_uuid()

        next=True
        inv_repo_obj=InventoryRepo(session=self.session)

        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id

        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        inv_config = shop_config.get("product", {})
        prefix = inv_config.get("prefix", "PROD")
        start_from = inv_config.get("start_from", 1)

        raw_sequence = await inv_repo_obj.get_next_sequence(data.shop_id, start_from)
        ui_id_str = format_ui_id(prefix, start_from, raw_sequence)

        inv_res=await inv_repo_obj.create(data=CreateInventoryDbSchema(**inventorydata_toadd,id=inventory_id,ui_id=ui_id_str,is_active=False))
        ic("Inventeroy created response  => ",inv_res)
        next=inv_res
        if next and variants_toadd:
            variant_res=await inv_repo_obj.create_variant_bulk(datas=variants_toadd)
            ic("Variant created response => ",variant_res)
            next=variant_res
            
        if next:
            # Sync to Read DB
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            from infras.read_db.models.inventory_model import InventoryReadModel
            from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
            from core.utils.id_formatter import format_ui_id
            
            raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inventory_id))
            if raw_inventory:
                raw_inventory_dict = dict(raw_inventory)
                
                # Formatted ID Generation is handled before creation now, so just pass it on.
                raw_inventory_dict['ui_id'] = inv_res['ui_id']
                
                read_model = InventoryReadModel(**raw_inventory_dict)
                await InventoryReadDbRepo.create_inventory(read_model)
                
                await _send_activity_log(
                    shop_id=data.shop_id,
                    action="CREATE",
                    entity_id=inventory_id,
                    description=f"Added new product: {data.name}",
                    changes=[{"field": "name", "before": "", "after": str(data.name)}]
                )
        
        return next

 

    async def create_bulk(self,datas:List[CreateInventorySchema],added_by:str):
        datas_toadd=[]
        for data in datas:
            datas_toadd.append(
                Inventory(**data.model_dump(mode="json"),id=generate_uuid(),added_by=added_by)
            )
        
        return await InventoryRepo(session=self.session).create_bulk(datas=datas_toadd)
    


    async def update(self,data:UpdateInventorySchema):
        inv_repo = InventoryRepo(session=self.session)
        ic("UPDATE INVENTORY DATA FROM FRONTEND:", data.model_dump())
        res=await inv_repo.update(
            data=UpdateInventoryDbSchema(
                **data.model_dump(mode='json',exclude_none=True,exclude_unset=True)
            )
        )
        
        if res:
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            from infras.read_db.models.inventory_model import InventoryReadModel
            
            raw_inventory = await inv_repo.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=data.id))
            if raw_inventory:
                read_model = InventoryReadModel(**raw_inventory)
                await InventoryReadDbRepo.replace_inventory(read_model)
                
                # Activity Logging
                changes_list = []
                desc_changes = []
                for k, v in data.model_dump(exclude_none=True, exclude_unset=True).items():
                    if k not in ["id", "shop_id", "barcode"]:
                        desc_changes.append(f"{k} updated")
                        changes_list.append({"field": k, "before": "...", "after": str(v)})
                
                if desc_changes:
                    product_name = raw_inventory.get('name', 'Unknown')
                    await _send_activity_log(
                        shop_id=data.shop_id,
                        action="UPDATE",
                        entity_id=data.id,
                        description=f"Updated product '{product_name}': {', '.join(desc_changes)}",
                        changes=changes_list
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
        # Fetch old inventory item to log name
        old_inventory = await InventoryRepo(session=self.session).getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=data.id))
        res = await InventoryRepo(session=self.session).delete(data=data)
        if res:
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            await InventoryReadDbRepo.delete_inventory(inventory_id=data.id, shop_id=data.shop_id)
            
            product_name = old_inventory.get('name', 'Unknown') if old_inventory else 'Unknown'
            await _send_activity_log(
                shop_id=data.shop_id,
                action="DELETE",
                entity_id=data.id,
                description=f"Deleted product: {product_name}",
                changes=[{"field": "name", "before": str(product_name), "after": "DELETED"}]
            )
        return res
    
    async def get(self,data:GetAllInventorySchema):
        return await InventoryRepo(session=self.session).get(data=data)
    

    async def getby_shop_id(self,data:GetInventoryByShopIdSchema):
        return await InventoryRepo(session=self.session).getby_shop_id(data=data)
    
    async def getby_id(self,data:GetInventoryByIdSchema):
        return await InventoryRepo(session=self.session).getby_id(data=data)
        
    async def get_inventory_stats(self, shop_id: str = None):
        return await InventoryRepo(session=self.session).get_inventory_stats(shop_id=shop_id)
    
    async def bulk_check(self,data:BulkCheckInventorySchema)-> List[dict] | list:
        return await InventoryRepo(session=self.session).bulk_check(data=data)
    
    async def bulk_batch_check(self,shop_id:str,batches_id: List[str])-> List[dict] | list:
        return await InventoryRepo(session=self.session).bulk_batch_check(shop_id=shop_id,batches_id=batches_id)
    
    async def bulk_serialno_check(self,shop_id:str,serianos_id:str)-> List[dict] | list:
        return await InventoryRepo(session=self.session).bulk_serialno_check(shop_id=shop_id,serialnos_id=serianos_id)
    
    async def bulk_varient_check(self,shop_id:str,variants_id:list,additional_conditions: Optional[tuple]=()):
        return await InventoryRepo(session=self.session).bulk_varient_check(shop_id=shop_id,variants_id=variants_id,additional_conditions=additional_conditions)
    
    
    async def search(self, shop_id: str, query: str, limit: int = 5):
        return await InventoryRepo(session=self.session).search(shop_id=shop_id, query=query, limit=limit)

    async def verify(self,data:VerifySchema):
        data_tocheck=data.model_dump(mode='json',exclude=['shop_id'],exclude_none=True,exclude_unset=True)

        if not data_tocheck or len(data_tocheck)<1:
            return {'id':'','exists':False}

        res=await InventoryRepo(session=self.session).verify(data=data)

        return res