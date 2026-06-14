from hyperlocal_platform.infras.saga.main import AsyncInfraDbLocalSession
from hyperlocal_platform.infras.saga.repo import SagaStatesRepo
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema
from infras.primary_db.main import AsyncInventoryLocalSession
from infras.primary_db.services.purchase_service import PurchaseService
from icecream import ic
from fastapi.exceptions import HTTPException

class MessagingQueueSupplierProducer:
    def __init__(self, payload: dict, headers: dict, saga_datas: dict):
        self.payload = payload
        self.headers = headers
        self.saga_datas = saga_datas

    async def create_purchase_v2(self):
        async with AsyncInventoryLocalSession() as session:
            try:
                ic("Received reply for supplier verification. Executing create_purchase_v2")
                
                # Fetch supplier response from saga datas
                supplier_response = self.saga_datas.get('data', {}).get('suppliers')
                ic(supplier_response)

                # The saga_datas['data']['data'] has the original CreatePurchaseSchema payload
                data_dict = self.saga_datas.get('data', {}).get('data', {})
                if not data_dict:
                    ic("Original purchase data not found in saga_datas")
                    return {
                        "response": False,
                        "execution": None
                    }
                
                # Check if supplier was verified
                if not supplier_response:
                    ic("Supplier verification failed")
                    return {
                        "response": False,
                        "execution": None
                    }

                # supplier_response is likely a list or a dict, usually true or dict with info.
                # If it's a list from getby_id, it might be dict. We will just check truthy for now.
                
                # Reconstruct the schema
                if not data_dict.get('datas'):
                    data_dict['datas'] = {}
                data_dict['datas']['supplier_name'] = supplier_response.get('name', '')
                
                create_data = CreatePurchaseSchema(**data_dict)

                # Initialize PurchaseService
                purchase_service = PurchaseService(session=session)
                
                # Execute purchase creation
                res = await purchase_service.create_direct_purchase_v2(data=create_data)
                
                if res:
                    return {
                        "response": True,
                        "execution": None
                    }
                else:
                    return {
                        "response": False,
                        "execution": None
                    }
                    
            except Exception as e:
                ic(f"Error in create_purchase_v2: {e}")
                return {
                    "response": False,
                    "execution": None
                }
