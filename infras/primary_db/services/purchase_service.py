from models.service_models.base_service_model import BaseServiceModel
from ..repos.purchase_repo import PurchaseRepo
from typing import Optional,List
from ..models.purchase_model import Purchase,PurchaseInventoryProducts
from ..models.inventory_model import InventoryBatches,InventorySerialNumbers
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from ..repos.inventory_repo import InventoryRepo
from schemas.v1.db_schemas.purchase_schema import CreatePurchaseDbSchema,UpdatePurchaseDbSchema
from schemas.v1.request_schemas.inventory_schema import BulkCheckInventorySchema,InventoryBatchSchema,GetAllInventorySchema
from schemas.v1.db_schemas.inventory_schema import InventoryBatchDbSchema,InventorySerialNumberDbSchema
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,BulkCheckPurchaseSchema,GetPurchaseByShopIdSchema,GetPurchaseByIdSchema,GetPurchaseByInventoryIdSchema,GetPurchaseBySupplierIdSchema
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums,PurchaseViewsEnums
from .inventory_service import InventoryService
from icecream import ic
from typing import Union


class PurchaseService(BaseServiceModel):
    def __init__(self, session:AsyncSession):
        self.session=session
        self.purchase_repo_obj=PurchaseRepo(session=session)

    async def create(self,data:CreatePurchaseSchema):
        purchase_id=generate_uuid()
        purchase_view=True

        if data.type.value == PurchaseTypeEnums.PO_CREATE.value:
            purchase_view=False
        
        if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
            purchase_id=data.purchase_id

        if not purchase_id:
            ic("Purchase id Not Found")
            return False
        ic(purchase_id)
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
        
        if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
            po_checked_results=await PurchaseRepo(session=self.session).bulk_check_purchase_inv_products(BulkCheckPurchaseSchema(purchase_id=purchase_id,inventory_id=inventories_tocheck))
            if not po_checked_results or len(po_checked_results)!=len(data.products):
                ic("Po Check Failed")
                return False
            

        checked_results=await InventoryRepo(session=self.session).bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inventories_tocheck))
        
        

        ic(checked_results)


        if len(checked_results)!=len(data.products):
            ic("Invalid checked results")
            return False
        
        batch_toupdate={}
        batch_toadd=[]
        variant_toudate={}
        product_toupdate={}
        serialno_toupdate={}
        purchase_inv_product_toadd=[]
        serialno_toadd=[]
        
        ERROR_OCCURED=False
        for inv_res in checked_results:
            ic(inv_res)
            requested_data=formatted_req_inventories.get(inv_res['id'],None)
            ic(requested_data)
            if not requested_data:
                ic("Requested data not found")
                ERROR_OCCURED=True
                return False

            batch_id:str=requested_data.get('batch_id',None)
            batch:InventoryBatchSchema=requested_data['batch']
            inv_id:str=requested_data.get('inventory_id',None)
            variant_id:str=requested_data.get('variant_id',None)
            serial_id:list=requested_data.get("serialno_id",None)

            stocks:int=requested_data['stocks']
            received_stocks=stocks
            if data.type.value==PurchaseTypeEnums.PO_UPDATE.value and not requested_data['received_stocks']:
                ic("Received stock not found")
                ERROR_OCCURED=True
                return False
            
            if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
                stocks=requested_data['received_stocks']

            if data.type!=PurchaseTypeEnums.PO_CREATE:
                if inv_res['has_variant'] and not variant_id:
                    ic("There is no variant id")
                    ERROR_OCCURED=True
                    return None
                
                if inv_res['has_batch'] and not batch_id and not batch:
                    ic("There is no batch id")
                    ERROR_OCCURED=True
                    return None
                
                if inv_res['has_batch'] and not batch_id and batch:
                    new_batch_id=generate_uuid()
                    batch_data_toadd=InventoryBatchDbSchema(
                        id=new_batch_id,
                        shop_id=data.shop_id,
                        inventory_id=inv_id,
                        variant_id=variant_id,
                        name=batch['name'],
                        expiry_date=batch['expiry_date'],
                        manufacturing_date=batch['manufacturing_date'],
                        stocks=stocks  
                    )
                    batch_toadd.append(
                        InventoryBatches(**batch_data_toadd.model_dump())
                    )

                    serialno_data_toadd=InventorySerialNumberDbSchema(
                        id=generate_uuid(),
                        shop_id=data.shop_id,
                        inventory_id=inv_id,
                        batch_id=new_batch_id,
                        variant_id=variant_id,
                        serial_numbers=requested_data['serial_numbers']
                    )

                    serialno_toadd.append(InventorySerialNumbers(**serialno_data_toadd.model_dump()))

            if inv_res['has_variant'] and variant_id:
                variant_toudate[variant_id]=stocks
            
            if inv_res['has_batch'] and batch_id:
                batch_toupdate[batch_id]=stocks

            if (inv_res['has_serialno'] and serial_id) and len(requested_data.get('serial_numbers',[]) or [])==stocks:
                serialno_toupdate[serial_id]=requested_data['serial_numbers']
            
            product_toupdate[inv_id]=stocks

            purchase_inv_product_toadd.append(
                PurchaseInventoryProducts(
                    inventory_id=inv_id,
                    purchase_id=purchase_id,
                    variant_id=variant_id,
                    batch_id=batch_id,
                    stocks=stocks,
                    sell_price=requested_data['sell_price'],
                    buy_price=requested_data['buy_price'],
                    margin=requested_data['margin'],
                    received_stocks=received_stocks
                )
            )
        
        if ERROR_OCCURED:
            ic("Error Occured")
            return False
        
        ic(data)
        ic(batch_toupdate,variant_toudate,product_toupdate,purchase_inv_product_toadd,serialno_toupdate,serialno_toadd)
        data_toadd=CreatePurchaseDbSchema(
            **data.model_dump(mode='json'),
            id=purchase_id,
            purchase_view=purchase_view
        )
        pur_repo_obj=PurchaseRepo(session=self.session)
        NEXT=False
        if data.type!=PurchaseTypeEnums.PO_UPDATE:
            pur_res=await pur_repo_obj.create(data=data_toadd)
            NEXT=pur_res

            if NEXT:
                pur_inv_res=await pur_repo_obj.create_purchase_inv_bulk(data=purchase_inv_product_toadd)
                NEXT=pur_inv_res

        if data.type==PurchaseTypeEnums.PO_UPDATE:
            pur_update_res=await pur_repo_obj.update(data=UpdatePurchaseDbSchema(shop_id=data.shop_id,id=purchase_id,purchase_view=purchase_view))
            NEXT=pur_update_res

        if NEXT and data.type.value!=PurchaseTypeEnums.PO_CREATE.value:
            inv_repo_obj=InventoryRepo(session=self.session)
            await InventoryRepo(session=self.session).create_batch_bulk(datas=batch_toadd)
            await inv_repo_obj.create_serialno_bulk(datas=serialno_toadd)

            await inv_repo_obj.bulk_qty_update(data=product_toupdate,shop_id=data.shop_id)
            await inv_repo_obj.bulk_batch_qty_update(data=batch_toupdate,shop_id=data.shop_id)
            await inv_repo_obj.bulk_variant_qty_update(data=variant_toudate,shop_id=data.shop_id)
            await inv_repo_obj.bulk_add_serialno(data=serialno_toupdate,shop_id=data.shop_id)


        return NEXT


    async def update(self,data:dict,shop_id:str):
        ic(data.datas.id,data.datas.shop_id)
        purchase=await self.getby_id(purchase_id=data.datas.id,shop_id=data.datas.shop_id)
        ic(purchase)
        if not purchase:
            return False

        ic(len(purchase['datas']['products']),len(data.datas.products))
        if len(purchase['datas']['products'])!=len(data.datas.products):
            return False
        
        purchase_view=False
        if data.datas.type==PurchaseTypeEnums.PO_UPDATE.value.value:
            purchase_view=True
        
        inventory_update_res=await purchase_helper(session=self.session,data=data,shop_id=shop_id)
        ic(inventory_update_res)
        if not inventory_update_res:
            return False
        ic(purchase_view)
        data_toupdate=UpdatePurchaseDbSchema(
            datas=data.datas.model_dump(mode='json'),
            shop_id=data.datas.shop_id,
            id=data.datas.id,
            type=data.datas.type,
            purchase_view=purchase_view
        )

        res = await self.purchase_repo_obj.update(data=data_toupdate)
        return res
    

    async def delete(self,shop_id:str,id:str):
        return await self.purchase_repo_obj.delete(id=id,shop_id=shop_id)

    async def get(self,data:GetPurchaseByShopIdSchema):
        return await self.purchase_repo_obj.get(data=data)
    
    async def get_by_inventory_id(self,data:GetPurchaseByInventoryIdSchema):
        res=await self.purchase_repo_obj.getby_inventory_id(data=data)
        return res
    
    async def getby_supplier_id(self,data:GetPurchaseBySupplierIdSchema):
        res=await self.purchase_repo_obj.getby_supplier_id(data=data)
        return res



    async def getby_id(self,data:GetPurchaseByIdSchema):
        return await self.purchase_repo_obj.getby_id(data=data)

    async def search(self, query, limit = 5):
        return await self.purchase_repo_obj.search(query=query,limit=limit)
    
    