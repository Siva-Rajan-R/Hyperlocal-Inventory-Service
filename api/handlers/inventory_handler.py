from infras.primary_db.services.inventory_service import InventoryService
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.exceptions import HTTPException
from schemas.v1.request_schemas.inventory_schema import CreateInventorySchema,UpdateInventorySchema,DeleteInventorySchema,GetAllInventorySchema,GetInventoryByIdSchema,GetInventoryByShopIdSchema,VerifySchema
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,BaseResponseTypDict,ErrorResponseTypDict
from messaging.saga_producer import SagaProducer,CreateSagaStateSchema,SagaStatusEnum
from hyperlocal_platform.core.enums.saga_state_enum import SagaStepsValueEnum
from hyperlocal_platform.core.typed_dicts.saga_status_typ_dict import SagaStateExecutionTypDict
from hyperlocal_platform.core.enums.routingkey_enum import RoutingkeyActions,RoutingkeyState,RoutingkeyVersions
from hyperlocal_platform.core.utils.routingkey_builder import generate_routingkey
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from core.constants import SERVICE_NAME
from infras.read_db.services.inventory_service import ReadDbInventoryService
from core.data_formats.enums.inventory_enums import InventoryFetchMode
from core.utils.calculate_offer import calculate_offer
from core.utils.validate_offer import validate_offer_input
from icecream import ic


class HandleInventoryRequest:
    def __init__(self,session:AsyncSession):
        self.session=session
    
    async def create(self,data:CreateInventorySchema):
        """
        Instead of creation, we need to trigger the event that event will handle the adding
        """ 
        
        res=await InventoryService(session=self.session).create(data=data,added_by="")
        ic(res)
        return res
    
    async def update(self,data:UpdateInventorySchema):
        res=await InventoryService(session=self.session).update(data=data)
        ic(res)
        return res
    
    async def delete(self,data:DeleteInventorySchema):
        res=await InventoryService(session=self.session).delete(data=data)
        return res
    
    async def get(self,data:GetAllInventorySchema):
        res=await InventoryService(session=self.session).get(data=data)

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventories fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )
    async def getby_shop_id(self,data:GetInventoryByShopIdSchema):
        res=await InventoryService(session=self.session).getby_shop_id(data=data)

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventories fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )
    
    async def getby_id(self,data:GetInventoryByIdSchema):
            res = await InventoryService(session=self.session).getby_id(data=data)

            return SuccessResponseTypDict(
                detail=BaseResponseTypDict(
                    msg="Inventory fetched successfully",
                    status_code=200,
                    success=True
                ),
                data=res
            )