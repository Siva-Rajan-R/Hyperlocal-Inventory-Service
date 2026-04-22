from models.service_models.base_service_model import BaseServiceModel
from ..repos.purchase_repo import PurchaseRepo
from typing import Optional,List
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from schemas.v1.db_schemas.purchase_schema import CreatePurchaseDbSchema,UpdatePurchaseDbSchema
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,UpdatePurchaseSchema
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums
from .inventory_service import InventoryService
from icecream import ic


class PurchaseService(BaseServiceModel):
    def __init__(self, session:AsyncSession):
        self.session=session
        self.purchase_repo_obj=PurchaseRepo(session=session)

    async def create(self,data:CreatePurchaseSchema,added_by:str,shop_id:str):
        purchase_id=generate_uuid()
        purchase_view=True
        barcodes=[]
        stock_toupdate={}
        sellprice_toupdate={}
        buyprice_toupdate={}

        for product in data.datas.products:
            barcodes.append(product.barcode)
            stock_toupdate[product.barcode]=product.quantity
            sellprice_toupdate[product.barcode]=product.sell_price
            buyprice_toupdate[product.barcode]=product.buy_price


        ic(barcodes,stock_toupdate)
        inventory_service_obj=InventoryService(session=self.session)
        checked_results=await inventory_service_obj.bulk_check(barcodes=barcodes,shop_id=shop_id)
        if len(checked_results)!=len(barcodes):
            return False
        
        if data.datas.type.value == PurchaseTypeEnums.PO_CREATE.value:
            purchase_view=False

        data_toadd=CreatePurchaseDbSchema(
            datas=data.datas.model_dump(mode='json'),
            id=purchase_id,
            added_by=added_by,
            shop_id=shop_id,
            purchase_view=purchase_view,
            type=data.datas.type
        )
        res=await self.purchase_repo_obj.create(data=data_toadd)
        ic(res)

        if res and len(stock_toupdate)>0:
            ic("hello ullai")
            await InventoryService(session=self.session).update_qty_bulk(shop_id=shop_id,data=stock_toupdate)
            await InventoryService(session=self.session).update_buyprice_bulk(shop_id=shop_id,data=buyprice_toupdate)
            await InventoryService(session=self.session).update_sellprice_bulk(shop_id=shop_id,data=sellprice_toupdate)
    
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
        ic(purchase_view)
        data_toupdate=UpdatePurchaseDbSchema(
            datas=data.datas.model_dump(mode='json'),
            shop_id=data.datas.shop_id,
            id=data.datas.id,
            type=data.datas.type,
            purchase_view=purchase_view
        )

        res = await self.purchase_repo_obj.update(data=data_toupdate)
        ic(res)
        if res:
            barcodes=[]
            stock_toupdate={}
            sellprice_toupdate={}
            buyprice_toupdate={}

            for product in purchase['datas']['products']:
                barcodes.append(product['barcode'])
                stock_toupdate[product['barcode']]=product['quantity']
                sellprice_toupdate[product['barcode']]=product['sell_price']
                buyprice_toupdate[product['barcode']]=product['buy_price']

                ic(barcodes,stock_toupdate,sellprice_toupdate,buyprice_toupdate)
            
            await InventoryService(session=self.session).update_qty_bulk(shop_id=shop_id,data=stock_toupdate)
            await InventoryService(session=self.session).update_buyprice_bulk(shop_id=shop_id,data=buyprice_toupdate)
            await InventoryService(session=self.session).update_sellprice_bulk(shop_id=shop_id,data=sellprice_toupdate)
        
        return res
    

    async def delete(self,shop_id:str,id:str):
        return await self.purchase_repo_obj.delete(id=id,shop_id=shop_id)

    async def get(self,timezone:TimeZoneEnum,shop_id:str,type:str,limit:Optional[int]=None,offset:Optional[int]=None,query:Optional[str]=""):
        return await self.purchase_repo_obj.get(timezone=timezone,shop_id=shop_id,type=type,query=query,limit=limit,offset=offset)


    async def getby_id(self,purchase_id:str,shop_id:str):
        return await self.purchase_repo_obj.getby_id(purchase_id=purchase_id,shop_id=shop_id)

    async def search(self, query, limit = 5):
        return await self.purchase_repo_obj.search(query=query,limit=limit)
    
    