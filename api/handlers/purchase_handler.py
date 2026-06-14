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
        if data.type.value!=PurchaseTypeEnums.DIRECT.value:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error : Creating Purchase",
                    status_code=400,
                    description=f"Invalid types, type should be direct",
                    success=False
                )
            )
        
        saga_id=generate_uuid()
        saga_data={
            'data':data.model_dump(mode="json"),
            'additional_data':{'shop_id':data.shop_id}
        }

        await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.PENDING,
                type="create",
                steps={
                    'supplier.verify':SagaStepsValueEnum.PENDING
                },
                execution=SagaStateExecutionTypDict(
                    step='supplier.verify',
                    service="SUPPLIERS"
                ),
                data=saga_data
            ),
            routing_key="suppliers.service.routing.key",
            exchange_name="suppliers.service.exchange",
            headers={
                'saga_id':saga_id,
                'reply_key': "purchase.producer.routing.key",
                'reply_exchange': 'purchase.producer.exchange',
                'reply_entity_name': 'create_purchase_v2',
                'reply_service_name': 'PURCHASE',
                'entity_name': 'get_supplier_by_id',
                'service_name': 'SUPPLIERS',
                'body': {
                    "id": data.supplier_id,
                    "shop_id": data.shop_id
                }
            }
        )

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Purchase Creation Request Accepted",
                status_code=202,
                success=True
            )
        )
    

    async def update(self,data:CreatePurchaseSchema,user_id:str):
        if data.type.value == PurchaseTypeEnums.PO_UPDATE.value:
            res=await self.purchase_service_obj.update(data=data.model_dump(mode='json'),shop_id=data.shop_id)
        else:
            res=await self.purchase_service_obj.edit_direct_purchase(data=data,user_id=user_id)

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
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo, PurchaseStatsReadDbRepo
        res=await PurchaseReadDbRepo.get_all_purchases(data=data)
        stats_res=await PurchaseStatsReadDbRepo.get_stats(shop_id=data.shop_id)
        if stats_res and "_id" in stats_res:
            stats_res["_id"] = str(stats_res["_id"])
        
        ic(res)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data={
                "purchases": res,
                "overall_stats": stats_res
            }
        )
    
    async def getby_id(self,data:GetPurchaseByIdSchema):
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
        res=await PurchaseReadDbRepo.get_purchase_by_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data=res.model_dump(mode="json") if res else None
        )
    

    async def get_by_inventory_id(self,data:GetPurchaseByInventoryIdSchema):
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
        res= await PurchaseReadDbRepo.get_purchases_by_inventory_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data=res
        )
    
    async def getby_supplier_id(self,data:GetPurchaseBySupplierIdSchema):
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
        res= await PurchaseReadDbRepo.get_purchases_by_supplier_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Purchase fetched successfully"
            ),
            data=res
        )
    
    async def get_supplier_stats(self, shop_id: str, supplier_id: str):
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
        res = await PurchaseReadDbRepo.get_supplier_stats(shop_id=shop_id, supplier_id=supplier_id)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                status_code=200,
                success=True,
                msg="Supplier stats fetched successfully"
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
        
    async def search(self, shop_id: str, query: str, limit: int = 5):
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
        from schemas.v1.request_schemas.purchase_schema import GetPurchaseByShopIdSchema
        req_data = GetPurchaseByShopIdSchema(shop_id=shop_id, query=query, limit=limit, offset=1)
        res = await PurchaseReadDbRepo.get_all_purchases(data=req_data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Purchase fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )
        
