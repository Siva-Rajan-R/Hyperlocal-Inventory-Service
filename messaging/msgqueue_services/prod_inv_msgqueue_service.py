from infras.primary_db.repos.inventory_repo import InventoryRepo
from infras.primary_db.repos.product_repo import ProductRepo
from infras.primary_db.services.prod_inv_service import ProductInventoryService
from schemas.v1.product_schemas.db_schemas import UpdateProductBatchDbSchema,UpdateProductSerialnoDbSchema
from schemas.v1.product_schemas.request_schemas import CreateProductBatchSchema,CreateProductSerialnoSchema,VerifyCombinedSchema,GetBulkProductsById
from schemas.v1.prod_inv_schemas.request_schemas import CreateProdInvBatchSerialnoSchema,CreateUpdateInvAll,UpdateAllProdInvSchema
from schemas.v1.inventory_schemas.request_schemas import VerifyInventoryCombinedSchema
from schemas.v1.inventory_schemas.request_schemas import VerifyInventoryCombinedSchema
from schemas.v1.inventory_schemas.db_schemas import UpdateInventoryPricingDbSchema,UpdateInventoryReorderPointDbSchema,UpdateInventoryStockDbSchema,UpdateInventoryStorageLocationDbSchema
from models.service_models.base_service_model import BaseServiceModel
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,ErrorResponseTypDict,BaseResponseTypDict
from fastapi.exceptions import HTTPException
from infras.primary_db.main import AsyncInventoryLocalSession
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from typing import Optional,Union,List,Dict
from icecream import ic
from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo

class MessagingQueueProductInvService:
  

    async def create_bulk_serialno(self,data:Union[List[CreateProductSerialnoSchema],List[dict]]):
        final_data=[]
        if isinstance(data[0], dict):
            for d in data:
                final_data.append(CreateProductSerialnoSchema(**d))
        async with AsyncInventoryLocalSession() as session:
            prod_inv_service_obj=ProductInventoryService(session=session)
            res=await prod_inv_service_obj.create_bulk_serialno(data=data)
            ic(res)
            if not res:
                return res

            return res
    
    async def create_bulk_batch(self,data:Union[List[CreateProductBatchSchema],List[dict]]):
        final_data=[]
        if isinstance(data[0], dict):
            for d in data:
                final_data.append(CreateProductBatchSchema(**d))

        async with AsyncInventoryLocalSession() as session:
            prod_inv_service_obj=ProductInventoryService(session=session)
            res=await prod_inv_service_obj.create_bulk_batch(data=data)
            ic(res)
            if not res:
                return res
            
            return res
    
    async def update_bulk_batch(self,data:Union[List[UpdateProductBatchDbSchema],List[dict]]):
        final_data=[]
        if isinstance(data[0], dict):
            for d in data:
                final_data.append(UpdateProductBatchDbSchema(**d))

        async with AsyncInventoryLocalSession() as session:
            prod_repo_obj=ProductRepo(session=session)
            res=await prod_repo_obj.update_bulk_batch(data=data)

            if not res:
                return res
            
            return res
        

    async def update_bulk_serialno(
        self,
        data: Union[List[UpdateProductSerialnoDbSchema], List[dict]]
    ):
        if data and isinstance(data[0], dict):
            data = [UpdateProductSerialnoDbSchema(**d) for d in data]

        async with AsyncInventoryLocalSession() as session:
            repo = ProductRepo(session=session)

            res = await repo.update_bulk_serialno(data=data)

            if not res:
                return res

            return res
        
    
    async def update_bulk_pricing(
        self,
        data: Union[List[UpdateInventoryPricingDbSchema], List[dict]]
    ):
        if data and isinstance(data[0], dict):
            data = [UpdateInventoryPricingDbSchema(**d) for d in data]

        async with AsyncInventoryLocalSession() as session:
            repo = InventoryRepo(session=session)

            res = await repo.update_bulk_pricing(data=data)

            if not res:
                return res

            return res

    # ---------------- STORAGE LOCATION ---------------- #

    async def update_bulk_storage_location(
        self,
        data: Union[List[UpdateInventoryStorageLocationDbSchema], List[dict]]
    ):
        if data and isinstance(data[0], dict):
            data = [UpdateInventoryStorageLocationDbSchema(**d) for d in data]

        async with AsyncInventoryLocalSession() as session:
            repo = InventoryRepo(session=session)

            res = await repo.update_bulk_storage_location(data=data)

            if not res:
                return res

            return res

    # ---------------- STOCK ---------------- #

    async def update_bulk_stock(
        self,
        data: Union[List[UpdateInventoryStockDbSchema], List[dict]]
    ):
        if data and isinstance(data[0], dict):
            data = [UpdateInventoryStockDbSchema(**d) for d in data]

        async with AsyncInventoryLocalSession() as session:
            repo = InventoryRepo(session=session)

            res = await repo.update_bulk_stocks(data=data)

            if not res:
                return res

            return res

    # ---------------- REORDER POINT ---------------- #

    async def update_bulk_reorder_point(
        self,
        data: Union[List[UpdateInventoryReorderPointDbSchema], List[dict]]
    ):
        if data and isinstance(data[0], dict):
            data = [UpdateInventoryReorderPointDbSchema(**d) for d in data]

        async with AsyncInventoryLocalSession() as session:
            repo = InventoryRepo(session=session)

            res = await repo.update_bulk_reorder_point(data=data)

            if not res:
                return res

            return res
        

    async def create_batch_serialno(self,data:Union[List[CreateProdInvBatchSerialnoSchema],dict]):
        if data and isinstance(data[0], dict):
            data = [CreateProdInvBatchSerialnoSchema(**d) for d in data]
        async with AsyncInventoryLocalSession() as session:
            service_obj = ProductInventoryService(session=session)

            res = await service_obj.create_batch_serialno(data=data)
            ic(res)
            if not res:
                return res

            return res
        

    async def verify_combined(self,data:Union[VerifyCombinedSchema,dict]):
        if data and isinstance(data, dict):
            data = VerifyCombinedSchema(**data)
        async with AsyncInventoryLocalSession() as session:
            repo = ProductRepo(session=session)

            res = await repo.verify_combined(data=data)
            ic(res)
            if not res:
                return res

            return res
        
    async def verify_inventory_combined(self, data: Union[VerifyInventoryCombinedSchema, dict]):
        ic("Verify_insidecombine ")
        if data and isinstance(data, dict):
            data = VerifyInventoryCombinedSchema(**data)
        async with AsyncInventoryLocalSession() as session:
            repo = InventoryRepo(session=session)

            res = await repo.verify_inventory_combined(data=data)
            ic(res)
            if not res:
                return res

            return res

    async def create_update_inventory_all(self,data:Union[CreateUpdateInvAll,dict]):
        if data and isinstance(data, dict):
            data = CreateUpdateInvAll(**data)

        async with AsyncInventoryLocalSession() as session:
            service_obj = ProductInventoryService(session=session)

            res = await service_obj.create_update_inventory_all(data=data)
            ic(res)
            if not res:
                return res

            return res


    async def get_bulk_product_by_id(self,data:Union[GetBulkProductsById,dict]):
        if data and isinstance(data, dict):
            data = GetBulkProductsById(**data)

        async with AsyncInventoryLocalSession() as session:
            # repo_obj = ProductRepo(session=session)

            # res = await repo_obj.get_bulk_products_by_id(data=data)
            res = await ProdInvReadDbRepo.get_bulk_by_id(data=data)
            ic(res)
            if not res:
                return res

            return res
        

    async def update_bulk_prodinv(self,data:Union[List[UpdateAllProdInvSchema],dict]):
        if data and isinstance(data[0], dict):
            data = [UpdateAllProdInvSchema(**d) for d in data]

        async with AsyncInventoryLocalSession() as session:
            repo_obj = ProductInventoryService(session=session)

            res = await repo_obj.update_all(data=data)
            ic(res)
            if not res:
                return res

            return res

