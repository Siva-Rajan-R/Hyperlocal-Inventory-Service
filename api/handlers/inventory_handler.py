from infras.primary_db.services.inventory_service import InventoryService
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.exceptions import HTTPException
from schemas.v1.request_schemas.inventory_schema import AddInventorySchema,UpdateInventorySchema
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


class HandleInventoryRequest:
    def __init__(self,session:AsyncSession):
        self.session=session
    
    async def create(self,data:AddInventorySchema,account_id:str):
        """
        Instead of creation, we need to trigger the event that event will handle the adding
        """ 
        if (await InventoryService(session=self.session).getby_id(inventory_barcode_id=data.datas.barcode,shop_id=data.datas.shop_id,timezone=TimeZoneEnum.Asia_Kolkata)):
            raise HTTPException(
                status_code=409,
                detail=ErrorResponseTypDict(
                    status_code=409,
                    msg="Error : Creating inventory",
                    description="A invantorty product is already exists",
                    success=False
                )
            )
        
        
        saga_id:str=generate_uuid()
        inventory_datas=data.datas.model_dump(mode="json")
        inventory_datas['account_id']=account_id

        data={'inventory':inventory_datas}

        return await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.PENDING,
                type="INVENTORY_CREATE",
                data=data,
                steps={
                    'shops.create':SagaStepsValueEnum.PENDING,
                    'products.create':SagaStepsValueEnum.PENDING
                },
                execution=SagaStateExecutionTypDict(
                    step="inventory:requested",
                    service=SERVICE_NAME.upper()
                )
            ),
            routing_key=generate_routingkey(
                domain=SERVICE_NAME,
                work_for=SERVICE_NAME,
                action=RoutingkeyActions.CREATE,
                state=RoutingkeyState.REQUESTED,
                version=RoutingkeyVersions.V1
            ),
            exchange_name="inventory.inventory.shops.exchange",
            headers={
                'saga_id':saga_id
            },
        )
    
    async def update(self,data:UpdateInventorySchema,account_id:str):
        """
        Instead of Updating, we need to trigger the event that event will handle the adding
        """
        # if data.offer_offline or data.offer_online:
        #     online=validate_offer_input(data.offer_offline)
        #     offline=validate_offer_input(data.offer_online)
        #     if not online or not offline:
        #         raise HTTPException(
        #             status_code=400,
        #             detail=ErrorResponseTypDict(
        #                status_code=400,
        #                msg="Error : Creating inventory",
        #                description="Enter a valid offer format",
        #                success=False
        #             )
        #         )
            
        
        if not await InventoryService(session=self.session).getby_id(inventory_barcode_id=data.datas.id,shop_id=data.datas.shop_id,timezone=TimeZoneEnum.Asia_Kolkata):
            raise HTTPException(
                status_code=404,
                detail=ErrorResponseTypDict(
                    msg="Error : Updating inventory",
                    description="Invalid shop or inventory id",
                    success=False,
                    status_code=404
                )
            )
        
        saga_id:str=generate_uuid()
        inventory_datas=data.datas.model_dump(mode="json")
        inventory_datas['account_id']=account_id

        data={'inventory':inventory_datas}

        return await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.PENDING,
                type="INVENTORY_UPDATE",
                data=data,
                steps={
                    'shops.update':SagaStepsValueEnum.PENDING,
                    'products.update':SagaStepsValueEnum.PENDING
                },
                execution=SagaStateExecutionTypDict(
                    step="inventory:requested",
                    service=SERVICE_NAME.upper()
                )
            ),
            routing_key=generate_routingkey(
                domain=SERVICE_NAME,
                work_for=SERVICE_NAME,
                action=RoutingkeyActions.UPDATE,
                state=RoutingkeyState.REQUESTED,
                version=RoutingkeyVersions.V1
            ),
            exchange_name="inventory.inventory.shops.exchange",
            headers={
                'saga_id':saga_id
            },
        )
    
    async def delete(self,inventory_id:str,shop_id:str):
        res=await InventoryService(session=self.session).delete(inventory_id=inventory_id,shop_id=shop_id)
        if res:
            await ReadDbInventoryService(payload={},conditions={'inventory_id':inventory_id,'shop_id':shop_id}).delete()
            return SuccessResponseTypDict(
                detail=BaseResponseTypDict(
                    msg="Inventory deleted successfully",
                    status_code=200,
                    success=True
                )
            )
        
        raise HTTPException(
            status_code=404,
            detail=ErrorResponseTypDict(
                msg="Erro : Deleting Inventory",
                description="Invalid inventory or shop id",
                success=False,
                status_code=404
            )
        )
    
    async def get(self,shop_id:str,timezone:TimeZoneEnum,offset:int,query:str="",limit:Optional[int]=10,read_db:Optional[bool]=True):
        
        res=await ReadDbInventoryService(payload={},conditions={}).get(query=query,limit=limit,offset=offset)
        if not read_db:
            res=await InventoryService(session=self.session).get(
                timezone=timezone,
                query=query,
                limit=limit,
                offset=offset,
                shop_id=shop_id
            )
    
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventories fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )
    
    async def getby_id(self,inventory_id:str,shop_id:str,timezone:TimeZoneEnum,read_db:Optional[bool]=True):
        res=await ReadDbInventoryService(payload={},conditions={}).get_one(queries={'inventory_id':inventory_id,'shop_id':shop_id})
        if not read_db:
            res = await InventoryService(session=self.session).getby_id(
                inventory_barcode_id=inventory_id,shop_id=shop_id,timezone=timezone
            )

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventory fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )