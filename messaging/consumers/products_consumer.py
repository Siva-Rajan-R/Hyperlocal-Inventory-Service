from models.messaging_models.consumer_model import BaseConsumerModel
from infras.primary_db.services.inventory_service import InventoryService,AddInventorySchema,UpdateInventorySchema
from infras.primary_db.main import AsyncInventoryLocalSession
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from hyperlocal_platform.core.enums.error_enums import ErrorTypeSEnum
from hyperlocal_platform.core.typed_dicts.saga_status_typ_dict import SagaStateErrorTypDict
from hyperlocal_platform.core.typed_dicts.messaging_typdict import EventPublishingTypDict,SuccessMessagingTypDict
from hyperlocal_platform.core.utils.routingkey_builder import generate_routingkey,RoutingkeyActions,RoutingkeyState,RoutingkeyVersions
from hyperlocal_platform.core.utils.exception_serializer import serialize_exception
from infras.read_db.models.inventory_model import ReadDbInventoryCreateModel,ReadDbInventoryUpdateModel
from hyperlocal_platform.core.basemodels.readdb_model import ReadDbBaseModel
from icecream import ic

class ProductsConsumer(BaseConsumerModel):
    async def create(self):
        is_new=False
        compensation_payload=EventPublishingTypDict(
            exchange_name='inventory.inventory.products.exchange',
            routing_key=generate_routingkey(
                domain='inventory',work_for='inventory',action=RoutingkeyActions.CREATE,
                state=RoutingkeyState.FAILED,version=RoutingkeyVersions.V1
            ),
            payload={},
            headers=self.headers
        )
        try:
            ic(self.payload)
            event_data:dict=self.payload.get('data',{})
            product_data:dict=event_data.get('products')
            inventory_data:dict=event_data.get('inventory')
            is_new=product_data['is_new']
            ic(inventory_data)
            data=AddInventorySchema(**inventory_data)
            

            async with AsyncInventoryLocalSession() as session:
                res=await InventoryService(session=session).create(data=data,added_by=inventory_data['account_id'],product_data=product_data)
            
            if not res:
                raise BussinessError(
                    type=ErrorTypeSEnum.BUSSINESS_ERROR,
                    error=SagaStateErrorTypDict(
                        code=ErrorTypeSEnum.BUSSINESS_ERROR.value,
                        debug="Invalid paylod for creating the inventory",
                        user_msg="Invalid data"
                    ),
                    compensation=is_new,
                    compensation_payload=compensation_payload
                )
            res=res.model_dump(mode='json')
            ic(res)
            readdb_data=ReadDbInventoryCreateModel(
                inventory_id=res.get('id'),
                added_by=res.get('added_by'),
                **inventory_data
            )

            return SuccessMessagingTypDict(
                response=res,
                set_response=False,
                mark_completed=True,
                read_db=ReadDbBaseModel(
                    payload=readdb_data,
                    method='create'
                )
            )
        
        except (BussinessError,RetryableError,FatalError):
            raise

        except Exception as e:
            raise FatalError(
                type=ErrorTypeSEnum.FATAL_ERROR,
                error=SagaStateErrorTypDict(
                    code=ErrorTypeSEnum.FATAL_ERROR.value,
                    debug=serialize_exception(e),
                    user_msg="Something weent wrong, please try again later"
                ),
                compensation=is_new,
                compensation_payload=compensation_payload
            )
    
    async def update(self):
        event_data:dict=self.payload.get('data',{})
        product_data:dict=event_data.get('products')
        inventory_data:dict=event_data.get('inventory')

        ic(inventory_data)

        data=UpdateInventorySchema(**inventory_data)

        async with AsyncInventoryLocalSession() as session:
            res=await InventoryService(session=session).update(data=data,product_data=product_data)
        
        if not res:
            raise BussinessError(
                type=ErrorTypeSEnum.BUSSINESS_ERROR,
                error=SagaStateErrorTypDict(
                    code=ErrorTypeSEnum.BUSSINESS_ERROR.value,
                    debug="Invalid paylod for creating the inventory",
                    user_msg="Invalid data"
                ),
            )
        
        readdb_data=ReadDbInventoryUpdateModel(
            **inventory_data
        )
        return SuccessMessagingTypDict(
            response=res,
            set_response=False,
            mark_completed=True,
            read_db=ReadDbBaseModel(
                payload=readdb_data,
                method='update',
                condition={
                    'inventory_id':inventory_data.get('id'),
                    'shop_id':inventory_data.get('shop_id'),
                    'barcode':inventory_data.get('barcode')
                }
            )
        )
    

    async def delete(self):
        return await super().delete()