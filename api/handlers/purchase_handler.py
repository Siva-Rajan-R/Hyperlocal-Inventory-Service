from infras.primary_db.services.purchase_service import PurchaseService
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,UpdatePurchaseSchema
from typing import Optional,List
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,ErrorResponseTypDict,BaseResponseTypDict
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums
from icecream import ic
from fastapi.exceptions import HTTPException
from core.utils.validate_fields import convert_field_type,validate_fields
from infras.caching.models.purchase_model import PurchaseProductCacheModel,PurchaseProductCachingSchema,PurchaseSupplierCacheModel,PurchaseSupplierCachingSchema
from infras.primary_db.repos.inventory_repo import InventoryRepo
from messaging.saga_producer import SagaProducer,CreateSagaStateSchema,SagaStatusEnum,SagaStateExecutionTypDict
from hyperlocal_platform.core.enums.saga_state_enum import SagaStepsValueEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from hyperlocal_platform.core.utils.routingkey_builder import generate_routingkey,RoutingkeyState,RoutingkeyActions,RoutingkeyVersions

class HandlePurchaseRequest:
    def __init__(self,session:AsyncSession):
        self.session=session
        self.purchase_service_obj=PurchaseService(session=session)
        self.purchase_types=PurchaseTypeEnums._value2member_map_.values()

    async def create(self,data:CreatePurchaseSchema,added_by:str,shop_id:str):
        ic(self.purchase_types)
        
        if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    status_code=400,
                    msg="Error : Updating Purchase",
                    description="Purchase type should be PO CREATE, DIRECT OR PRODUCTION",
                    success=False
                )
            )
        # await validate_fields(service_name="PURCHASE",shop_id=data.shop_id,incoming_fields=data.datas)
        
        cached_datas=await PurchaseProductCacheModel(shop_id=shop_id,user_id=added_by).get()
        ic(cached_datas)
        inverntory_update_data={}
        data_to_check=[]

        if not cached_datas:
            cached_datas={'barcodes':[]}

        for product in data.datas['products']:
            if product['barcode'] not in cached_datas['barcodes']:
                data_to_check.append(product['barcode'])
            inverntory_update_data[product['barcode']]=product['qty']
        
        product_to_check=[]
        if len(data_to_check)>0:
            checked_prods=await InventoryRepo(session=self.session).bulk_check(barcodes=data_to_check,shop_id=shop_id)
            product_to_check.extend(list(set(data_to_check)^set(checked_prods)))

        if data.type.value==PurchaseTypeEnums.PO_CREATE.value:
            inverntory_update_data={}

        saga_steps={
            'products.create':SagaStepsValueEnum.PENDING
        }
        saga_id=generate_uuid()
        saga_data={
            'data':data,
            'product_to_check':product_to_check,
            'inventory_update_data':inverntory_update_data,
            'additional_data':{'shop_id':shop_id,'user_id':added_by}
        }
        saga_exchange_name="purchase.purchase.products.exchange"
        saga_routing_key=generate_routingkey(
            domain="PURCHASE",
            work_for="PURCHASE",
            action=RoutingkeyActions.CREATE,
            state=RoutingkeyState.REQUESTED,
            version=RoutingkeyVersions.V1
        )
        supplier_verify=False

        supplier_cached_data=await PurchaseSupplierCacheModel(shop_id=shop_id,user_id=added_by).get()
        ic(supplier_cached_data)
        if not supplier_cached_data and len(product_to_check)>0:
            ic("ullai 1")
            saga_data={**saga_data,"supplier_verify":True}
            saga_steps={**saga_steps,"supplier:create":SagaStepsValueEnum.PENDING}
            supplier_verify=True
        
        if not supplier_cached_data and len(product_to_check)==0:
            ic("ullai 2")
            saga_data={**saga_data,"supplier_verify":True}
            saga_steps={**saga_steps,"supplier:create":SagaStepsValueEnum.PENDING}
            saga_exchange_name="purchase.purchase.suppliers.exchange"
            supplier_verify=True
        
        ic(supplier_verify)

        

        if len(product_to_check)>0 or supplier_verify:
            return await SagaProducer.emit(
                saga_payload=CreateSagaStateSchema(
                    id=saga_id,
                    status=SagaStatusEnum.PENDING,
                    type="create",
                    steps=saga_steps,
                    execution=SagaStateExecutionTypDict(
                        step='purchase:requested',
                        service="PURCHASE"
                    ),
                    data=saga_data
                ),
                routing_key=saga_routing_key,
                exchange_name=saga_exchange_name,
                headers={
                    'saga_id':saga_id
                }
            )
        ic(inverntory_update_data)
        res=await self.purchase_service_obj.create(data=data,added_by=added_by,inventory_update_data=inverntory_update_data,shop_id=shop_id)

        if res:
            return SuccessResponseTypDict(
                detail=BaseResponseTypDict(
                    msg="Purchase created successfully",
                    status_code=201,
                    success=True
                )
            )
        
        raise HTTPException(
            status_code=400,
            detail=ErrorResponseTypDict(
                msg="Error : Creating Purchase",
                status_code=400,
                description=f"Invalid data types",
                success=False
            )
        )
    

    async def update(self,data:UpdatePurchaseSchema,user_id:str):
        ic(self.purchase_types)
        if data.type.value!=PurchaseTypeEnums.PO_UPDATE.value:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    status_code=400,
                    msg="Error : Updating Purchase",
                    description="Purchase type should be PO UPDATE",
                    success=False
                )
            )
        

        cached_datas=await PurchaseProductCacheModel(shop_id=data.shop_id,user_id=user_id).get()
        inverntory_update_data={}
        data_to_check=[]

        if not cached_datas:
            cached_datas={'barcodes':[]}

        for product in data.datas['products']:
            if product['barcode'] not in cached_datas['barcodes']:
                data_to_check.append(product['barcode'])
            inverntory_update_data[product['barcode']]=product['recived_qty']
        
        product_to_check=[]
        if len(data_to_check)>0:
            checked_prods=await InventoryRepo(session=self.session).bulk_check(barcodes=data_to_check,shop_id=data.shop_id)
            product_to_check.extend(list(set(data_to_check)^set(checked_prods)))
        

        if len(product_to_check)>0:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    description="Invalid Purchase id for updating the purchase",
                    msg="Error : Updating Purchase",
                    success=False,
                    status_code=400
                )
            )
        ic(inverntory_update_data)
        res=await self.purchase_service_obj.update(data=data,inventory_update_data=inverntory_update_data,shop_id=data.shop_id)

        if res:
            return SuccessResponseTypDict(
                detail=BaseResponseTypDict(
                    msg="Purchase updated successfully",
                    status_code=200,
                    success=True
                )
            )
        
        raise HTTPException(
            status_code=400,
            detail=ErrorResponseTypDict(
                msg="Error : Updating Purchase",
                status_code=400,
                description=f"Invalid data types",
                success=False
            )
        )
    

    async def delete(self,shop_id:str,id:str):
        res=await self.purchase_service_obj.delete(shop_id=shop_id,id=id)

        if res:
            return SuccessResponseTypDict(
                detail=BaseResponseTypDict(
                    msg="Purchase deleted successfully",
                    status_code=200,
                    success=True
                )
            )
        
        raise HTTPException(
            status_code=400,
            detail=ErrorResponseTypDict(
                msg="Error : Deleting Purchase",
                status_code=400,
                description=f"Invalid data types",
                success=False
            )
        )
    

    async def get(self,shop_id:str,timezone:TimeZoneEnum,type:PurchaseTypeEnums,limit:Optional[int]=None,offset:Optional[int]=None,query:Optional[str]=""):
        res=await self.purchase_service_obj.get(timezone=timezone,shop_id=shop_id,type=type,limit=limit,offset=offset,query=query)

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data=res
        )
    

    async def pre_purchase(self,data:CreatePurchaseSchema,user_id:str):
        data_to_check=[]
        is_prod_exists=await InventoryRepo(session=self.session).bulk_check(barcodes=[data.datas['products']['barcode']],shop_id=data.shop_id)
        if not is_prod_exists:
            data_to_check.append(data.datas['products']['barcode'])
        
        if (data_to_check)>0:
            saga_id=generate_uuid()
            saga_data={
                'data':data,
                'product_to_check':{},
                'inventory_update_data':{},
                'additional_data':{'shop_id':data.shop_id,'user_id':user_id}
            }

            return await SagaProducer.emit(
                saga_payload=CreateSagaStateSchema(
                    id=saga_id,
                    status=SagaStatusEnum.PENDING,
                    type="create",
                    steps={
                        'products.create':SagaStepsValueEnum.PENDING
                    },
                    execution=SagaStateExecutionTypDict(
                        step='purchase:requested',
                        service="PURCHASE"
                    ),
                    data=saga_data
                ),
                routing_key=generate_routingkey(
                    domain="PURCHASE",
                    work_for="PURCHASE",
                    action=RoutingkeyActions.CREATE,
                    state=RoutingkeyState.REQUESTED,
                    version=RoutingkeyVersions.V1
                ),
                exchange_name="purchase.purchase.products.exchange",
                headers={
                    'saga_id':saga_id
                }
            )
        
