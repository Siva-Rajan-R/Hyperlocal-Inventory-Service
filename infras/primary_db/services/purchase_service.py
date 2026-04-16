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

    async def create(self,data:CreatePurchaseSchema,added_by:str,inventory_update_data:dict,shop_id:str):
        purchase_id=generate_uuid()
        purchase_view=True
        if data.type.value == PurchaseTypeEnums.PO_CREATE.value:
            purchase_view=False
        data_toadd=CreatePurchaseDbSchema(**data.model_dump(mode='json'),purchase_view=purchase_view,added_by=added_by,id=purchase_id)
        res=await self.purchase_repo_obj.create(data=data_toadd)
        ic(res,inventory_update_data)

        if res and len(inventory_update_data)>0:
            ic("hello ullai")
            return await InventoryService(session=self.session).update_qty_bulk(shop_id=shop_id,data=inventory_update_data)
    
        return res
    

    async def update(self,data:UpdatePurchaseSchema,inventory_update_data:dict,shop_id:str):
        ic("before inv update data",inventory_update_data)
        ic(data.id,data.shop_id)
        purchase=await self.getby_id(purchase_id=data.id,shop_id=data.shop_id)
        ic(purchase)
        if not purchase:
            return False
        ic(purchase)
        ic(len(purchase['datas']['products']),len(data.datas['products']))
        if len(purchase['datas']['products'])!=len(data.datas['products']):
            return False
        
        purchase_view=False
        if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
            purchase_view=True

        data_toupdate=UpdatePurchaseDbSchema(
            **data.model_dump(mode='json'),
            purchase_view=purchase_view
        )

        res = await self.purchase_repo_obj.update(data=data_toupdate)
        ic(res)
        if res:
            ic("After inv update data",inventory_update_data)
            return await InventoryService(session=self.session).update_qty_bulk(shop_id=shop_id,data=inventory_update_data)
    

    async def delete(self,shop_id:str,id:str):
        return await self.purchase_repo_obj.delete(id=id,shop_id=shop_id)

    async def get(self,timezone:TimeZoneEnum,shop_id:str,type:str,limit:Optional[int]=None,offset:Optional[int]=None,query:Optional[str]=""):
        return await self.purchase_repo_obj.get(timezone=timezone,shop_id=shop_id,type=type,query=query,limit=limit,offset=offset)


    async def getby_id(self,purchase_id:str,shop_id:str):
        return await self.purchase_repo_obj.getby_id(purchase_id=purchase_id,shop_id=shop_id)

    async def search(self, query, limit = 5):
        return await self.purchase_repo_obj.search(query=query,limit=limit)
    
    