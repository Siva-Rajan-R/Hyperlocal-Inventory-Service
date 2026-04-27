from models.service_models.base_service_model import BaseServiceModel
from ..repos.purchase_repo import PurchaseRepo
from typing import Optional,List
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from ..repos.inventory_repo import InventoryRepo
from schemas.v1.db_schemas.purchase_schema import CreatePurchaseDbSchema,UpdatePurchaseDbSchema
from schemas.v1.db_schemas.inventory_schema import InventoryBatchDbSchema
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,UpdatePurchaseSchema
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums,PurchaseViewsEnums
from .inventory_service import InventoryService
from icecream import ic
from typing import Union


async def purchase_helper(session: AsyncSession, data: Union[CreatePurchaseSchema, UpdatePurchaseSchema], shop_id: str):
    inventory_tocheck=[]
    varients_tocheck=[]
    batches_toupdadd={}

    variants_stock_toupdate={}
    variants_sellprice_toupdate={}
    variants_buyprice_toupdate={}
    varient_serialnumber_toupdate={}
    variant_batch_toupdadd={}
    
    stock_toupdate={}
    sellprice_toupdate={}
    buyprice_toupdate={}
    serialnumber_toupdate={}

    varients_checked_results=[]
    inv_checked_results=[]

    

    for product in data.datas.products:
        product_qty=product.quantity if not product.received_qty else product.received_qty
        
        if product.varient_id:
            varients_tocheck.append(product.varient_id)
        else:
            inventory_tocheck.append(product.barcode)

        stock_toupdate[product.barcode]=product_qty
        sellprice_toupdate[product.barcode]=product.sell_price
        buyprice_toupdate[product.barcode]=product.buy_price
        if len(product.serial_numbers)!=product_qty:
            return False
        serialnumber_toupdate[product.barcode]=product.serial_numbers
        batches_toupdadd[product.barcode]=InventoryBatchDbSchema(
            id=generate_uuid(),
            shop_id=shop_id,
            stocks=product_qty,
            inventory_id=product.inventory_id,
            name=product.batch_name,
            expiry_date=product.batches.expiry_date,
            manufacturing_date=product.batches.manufacturing_date
        )

        variants_stock_toupdate[product.varient_id]=product_qty
        variants_sellprice_toupdate[product.varient_id]=product.sell_price
        variants_buyprice_toupdate[product.varient_id]=product.buy_price
        if len(product.serial_numbers)!=product_qty:
            return False
        varient_serialnumber_toupdate[product.varient_id]=product.serial_numbers

        if product.varient_id:
            variant_batch_toupdadd[product.varient_id] = InventoryBatchDbSchema(
                id=generate_uuid(),
                shop_id=shop_id,
                stocks=product_qty,
                inventory_id=product.inventory_id,
                variant_id=product.varient_id,
                name=product.batch_name,
                expiry_date=product.batches.expiry_date,
                manufacturing_date=product.batches.manufacturing_date
            )


    ic(inventory_tocheck)
    ic(varients_tocheck)
    inventory_service_obj=InventoryService(session=session)
    if len(inventory_tocheck)>0:
        
        inv_checked_results=await inventory_service_obj.bulk_check(barcodes=inventory_tocheck,shop_id=shop_id)

        ic(inv_checked_results)
        if (len(inv_checked_results)!=len(inventory_tocheck)):
            return False

    ic(varients_tocheck)
    if len(varients_tocheck)>0:
        varients_checked_results=await inventory_service_obj.bulk_varient_check(shop_id=shop_id,variants_id=varients_tocheck)
        
        ic(varients_checked_results)
        if (len(varients_checked_results)!=len(varients_tocheck)):
            return False
    
    

    inventory_skipped_products=[]
    final_batches_toupdadd=[]

    for inventory_data in inv_checked_results:
        datas=inventory_data['datas']
        if datas['has_varients']:
            del stock_toupdate[datas['barcode']]
            del sellprice_toupdate[datas['barcode']]
            del buyprice_toupdate[datas['barcode']]
            del serialnumber_toupdate[datas['barcode']]
            del batches_toupdadd[datas['barcode']]
            
        else:
            final_batches_toupdadd.append(batches_toupdadd[datas['barcode']])
            inventory_skipped_products.append(inventory_data['id'])

    ic(inventory_skipped_products,final_batches_toupdadd)
    ic(stock_toupdate,sellprice_toupdate,buyprice_toupdate,serialnumber_toupdate,batches_toupdadd)

    for varient_data in varients_checked_results:
        datas=varient_data['datas']
        if varient_data['inventory_id'] in inventory_skipped_products:
            del variants_stock_toupdate[varient_data['id']]
            del variants_sellprice_toupdate[varient_data['id']]
            del variants_buyprice_toupdate[varient_data['id']]
            del varient_serialnumber_toupdate[varient_data['id']]
            del variant_batch_toupdadd[varient_data['id']]
        else:
            final_batches_toupdadd.append(variant_batch_toupdadd[varient_data['id']])
    
    ic(variants_stock_toupdate,variants_sellprice_toupdate,variants_buyprice_toupdate,varient_serialnumber_toupdate,variant_batch_toupdadd)
            

    if len(stock_toupdate)>0:
        await InventoryService(session=session).update_qty_bulk(shop_id=shop_id,data=stock_toupdate)
        await InventoryService(session=session).update_buyprice_bulk(shop_id=shop_id,data=buyprice_toupdate)
        await InventoryService(session=session).update_sellprice_bulk(shop_id=shop_id,data=sellprice_toupdate)
        await InventoryService(session=session).bulk_serialnumber_update(shop_id=shop_id,data=serialnumber_toupdate)
    
    if len(variants_stock_toupdate)>0:
        await InventoryRepo(session=session).bulk_variant_qty_update(shop_id=shop_id,data=variants_stock_toupdate)
        await InventoryRepo(session=session).bulk_variant_buyprice_update(shop_id=shop_id,data=variants_buyprice_toupdate)
        await InventoryRepo(session=session).bulk_variant_sellprice_update(shop_id=shop_id,data=variants_sellprice_toupdate)
        await InventoryRepo(session=session).bulk_variant_serialnumber_update(shop_id=shop_id,data=varient_serialnumber_toupdate)

    if len(final_batches_toupdadd)>0:
        await InventoryRepo(session=session).bulk_inventory_batch_qty_add_update(datas=final_batches_toupdadd)

    return True

class PurchaseService(BaseServiceModel):
    def __init__(self, session:AsyncSession):
        self.session=session
        self.purchase_repo_obj=PurchaseRepo(session=session)

    async def create(self,data:CreatePurchaseSchema,added_by:str,shop_id:str):
        purchase_id=generate_uuid()
        purchase_view=True

        if data.datas.type.value == PurchaseTypeEnums.PO_CREATE.value:
            purchase_view=False
        
        if data.datas.type.value == PurchaseTypeEnums.DIRECT.value:
            inventory_update_res=await purchase_helper(session=self.session,data=data,shop_id=shop_id)
            if not inventory_update_res:
                raise BussinessError(
                    status_code=400,
                    msg="Error : Creating purchase",
                    description="Invalid product data, Please check the products data",
                    success=False
                )

        data_toadd=CreatePurchaseDbSchema(
            datas=data.datas.model_dump(mode='json'),
            id=purchase_id,
            supplier_id=data.datas.supplier_id,
            added_by=added_by,
            shop_id=shop_id,
            purchase_view=purchase_view,
            type=data.datas.type
        )
        res=await self.purchase_repo_obj.create(data=data_toadd)
        return res


    async def update(self,data:UpdatePurchaseSchema,shop_id:str):
        ic(data.datas.id,data.datas.shop_id)
        purchase=await self.getby_id(purchase_id=data.datas.id,shop_id=data.datas.shop_id)
        ic(purchase)
        if not purchase:
            return False

        ic(len(purchase['datas']['products']),len(data.datas.products))
        if len(purchase['datas']['products'])!=len(data.datas.products):
            return False
        
        purchase_view=False
        if data.datas.type==PurchaseTypeEnums.PO_UPDATE.value:
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

    async def get(self,timezone:TimeZoneEnum,shop_id:str,view:str,limit:Optional[int]=None,offset:Optional[int]=None,query:Optional[str]=""):
        return await self.purchase_repo_obj.get(timezone=timezone,shop_id=shop_id,view=view,query=query,limit=limit,offset=offset)


    async def getby_id(self,purchase_id:str,shop_id:str):
        return await self.purchase_repo_obj.getby_id(purchase_id=purchase_id,shop_id=shop_id)

    async def search(self, query, limit = 5):
        return await self.purchase_repo_obj.search(query=query,limit=limit)
    
    