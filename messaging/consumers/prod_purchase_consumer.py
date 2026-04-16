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
from infras.primary_db.services.purchase_service import PurchaseService
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,UpdatePurchaseSchema
from icecream import ic

class ProductPurchaseConsumer(BaseConsumerModel):

    async def create(self):
        compensation_payload=EventPublishingTypDict(
            exchange_name="purchase.purchase.products.exchange",
            routing_key=generate_routingkey(
                domain="PURCHASE",
                work_for="PURCHASE",
                action=RoutingkeyActions.CREATE,
                state=RoutingkeyState.FAILED,
                version=RoutingkeyVersions.V1
            ),
            headers=self.headers,
            payload={}
        )
        is_new_exists=False
        try:
            ic(self.payload)
            

            data=self.payload['data']
            ic(data)
            products=data['data']['datas']['products']
            product_to_check=data['product_to_check']
            inventory_to_add=data['inventory_update_data']
            additional_data=data['additional_data']
            created_barcodes=data['products']['created_barcodes']

            is_new_exists=True if len(created_barcodes)>0 else False


            async with AsyncInventoryLocalSession() as session:
                data_toadd=CreatePurchaseSchema(**data['data'])
                inventory_to_create=[]
                ic(product_to_check)
                for product in products:
                    if product['barcode'] in product_to_check:
                        inventory_to_create.append(
                            AddInventorySchema(
                                shop_id=additional_data['shop_id'],
                                barcode=product['barcode'],
                                stocks=product['qty'],
                                buy_price=product['buy_price'],
                                sell_price=product['sell_price'],
                                datas={}
                            )
                        )

                        del inventory_to_add[product['barcode']]

                ic(inventory_to_create)
                if len(inventory_to_create)>0:
                    res=await InventoryService(session=session).create_bulk(datas=inventory_to_create,added_by=additional_data['user_id'])
                ic(inventory_to_add)
                res=await PurchaseService(session=session).create(
                    data=data_toadd,
                    added_by=additional_data['user_id'],
                    shop_id=additional_data['shop_id'],
                    inventory_update_data=inventory_to_add
                )
                ic("final res",res)
                if not res:
                    raise FatalError(
                        type=ErrorTypeSEnum.FATAL_ERROR,
                        error=SagaStateErrorTypDict(
                            code="FATAL ERROR",
                            debug="Something went wrong while creating the Purchase",
                            user_msg="Invalid datas"
                        ),
                        compensation_payload=compensation_payload,
                        compensation=is_new_exists
                    )
                
            return SuccessMessagingTypDict(
                response=res,
                mark_completed=True
            )

        except (BussinessError,FatalError,RetryableError):
            raise

        except Exception as e:
            raise FatalError(
                type=ErrorTypeSEnum.FATAL_ERROR,
                error=SagaStateErrorTypDict(
                    code=ErrorTypeSEnum.FATAL_ERROR.value,
                    debug=serialize_exception(e),
                    user_msg="Something weent wrong, please try again later"
                ),
                compensation_payload=compensation_payload,
                compensation=is_new_exists
            )
        

    async def update(self):
        return await self.create()
    

    async def delete(self):
        ...