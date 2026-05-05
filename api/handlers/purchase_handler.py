from infras.primary_db.services.purchase_service import PurchaseService
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,GetPurchaseByShopIdSchema,GetPurchaseByIdSchema,GetPurchaseByInventoryIdSchema,GetPurchaseBySupplierIdSchema
from typing import Optional,List
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,ErrorResponseTypDict,BaseResponseTypDict
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums,PurchaseViewsEnums
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

    async def create(self,data:CreatePurchaseSchema):

        res=await PurchaseService(session=self.session).create(data=data)
        
        if not res:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error : Creating Purchase",
                    status_code=400,
                    description=f"Invalid data types",
                    success=False
                )
            )
        
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Purchase Created successfully",
                status_code=201,
                success=True
            )
        )
    

    async def update(self,data:dict,user_id:str):
        ic(self.purchase_types)
        if data.datas.type!=PurchaseTypeEnums.PO_UPDATE.value:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    status_code=400,
                    msg="Error : Updating Purchase",
                    description="Purchase type should be PO UPDATE",
                    success=False
                )
            )
        

        res=await self.purchase_service_obj.update(data=data,shop_id=data.datas.shop_id)

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
    

    async def get(self,data:GetPurchaseByShopIdSchema):
        res=await self.purchase_service_obj.get(data=data)

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data=res
        )
    
    async def getby_id(self,data:GetPurchaseByIdSchema):
        res=await self.purchase_service_obj.getby_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data=res
        )
    

    async def get_by_inventory_id(self,data:GetPurchaseByInventoryIdSchema):
        res= await self.purchase_service_obj.get_by_inventory_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data=res
        )
    
    async def getby_supplier_id(self,data:GetPurchaseBySupplierIdSchema):
        res= await self.purchase_service_obj.getby_supplier_id(data=data)
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
        
