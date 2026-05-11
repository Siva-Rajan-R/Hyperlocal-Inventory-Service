from schemas.v1.request_schemas.billing_schema import BillingProductSchema,CreateBillingSchema
from infras.primary_db.main import AsyncSession
from infras.primary_db.services.inventory_service import InventoryService,BulkCheckInventorySchema
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,BaseResponseTypDict,ErrorResponseTypDict
from fastapi import HTTPException
from messaging.saga_producer import SagaProducer,CreateSagaStateSchema,SagaStatusEnum,SagaStateExecutionTypDict,SagaStateErrorTypDict
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from icecream import ic

class HandleBillingRequest:
    def __init__(self,session:AsyncSession):
        self.session=session

    async def create(self,data:CreateBillingSchema):
        
        inv_service_obj=InventoryService(session=self.session)
        inv_id=[]
        structured_data={}
        items=[]
        ERROR_OCCURED=None
        for product in data.products:
            inv_id.append(product.id)
            structured_data[product.id]=product.model_dump()
            

        res=await inv_service_obj.bulk_check(
            data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inv_id)
        )

        if len(inv_id)!=len(res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Products",
                    status_code=400
                )
            )
        
        variant_id=[]
        batch_id=[]
        serialno_id=[]

        for inv_res in res:
            ic(inv_res)
            ic(structured_data[inv_res['id']])
            if inv_res['has_variant'] and not structured_data[inv_res['id']]['variant_id']:
                ERROR_OCCURED=True
                break 

            if inv_res['has_batch'] and not structured_data[inv_res['id']]['batch_id']:
                ERROR_OCCURED=True
                break

            if inv_res['has_serialno'] and not structured_data[inv_res['id']]['serialno_id'] or len(structured_data[inv_res['id']]['serial_numbers'])!=structured_data[inv_res['id']]['quantity']:
                ERROR_OCCURED=True
                break

            if inv_res['has_variant']:
                variant_id.append(structured_data[inv_res['id']]['variant_id'])

            if inv_res['has_batch']:
                batch_id.append(structured_data[inv_res['id']]['batch_id'])

            if inv_res['has_serialno']:

                serialno_id.append(structured_data[inv_res['id']]['serialno_id'])

            structured_data[inv_res['id']]['buy_price']=inv_res['buy_price']
            structured_data[inv_res['id']]['sell_price']=inv_res['sell_price']
            structured_data[inv_res['id']]['barcode']=inv_res['barcode']
            structured_data[inv_res['id']]['gst']="0%"

            

        
        if ERROR_OCCURED:
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Variants or Invalid serial numbers",
                    status_code=400
                )
            )


        variant_res=await inv_service_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=variant_id)
        if len(variant_id)!=len(variant_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Variants",
                    status_code=400
                )
            )
        
        for variant in variant_res:
            structured_data[variant['inventory_id']]['buy_price']=variant['buy_price']
            structured_data[variant['inventory_id']]['sell_price']=variant['sell_price']
        
        batch_res=await inv_service_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=batch_id)
        if len(batch_id)!=len(batch_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Batch",
                    status_code=400
                )
            )
        
        serialno_res=await inv_service_obj.bulk_serialno_check(shop_id=data.shop_id,serianos_id=serialno_id)
        if len(serialno_id)!=len(serialno_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Batch",
                    status_code=400
                )
            )
        

        for key,val in structured_data.items():
            ic(key)
            ic(val)
            items.append(
                {
                    'inventory_id':val['id'],
                    'variant_id':val['variant_id'],
                    'batch_id':val['batch_id'],
                    'serialno_id':val['serialno_id'],
                    'barcode':val['barcode'],
                    'inv_serial_numbers':val['serial_numbers'],
                    'buy_price':val['buy_price'],
                    'sell_price':val['sell_price'],
                    'gst':val['gst'],
                    'quantity':val['quantity']
                }
            )
        
        exchange_name="orders.service.exchange"
        r_key="orders.serivce.routing.key"
        reply_key='billing.producer.routing.key'
        reply_exchange='billing.producer.exchange'
        reply_service_name="BILLING"
        reply_entity_name="create_billing"
        
        data_toadd=data.model_dump(mode='json')
        saga_id:str=generate_uuid()

        body={
            'shop_id':data.shop_id,
            'customer_id':data.customer_id,
            'status':'COMPLETED',
            'origin':'OFFLINE',
            'payment_method':data.payment_method,
            'items':items
        }

        headers={
            "reply_key":reply_key,
            "reply_exchange":reply_exchange,
            "reply_service_name":reply_service_name,
            "reply_entity_name":reply_entity_name,
            "service_name":"ORDERS",
            "entity_name":"create_order",
            "body":body

        }
        await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.IN_PROGRESS,
                type="SERVICE",
                data={'billing':data_toadd},
                steps={
                    'ORDER_CREATION':'PENDING'
                },
                execution=SagaStateExecutionTypDict(
                    step="ORDER_CREATION",service="BILLING"
                )
            ),
            headers=headers,
            exchange_name=exchange_name,
            routing_key=r_key
        )

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Billing Created Successfully",
                status_code=203,
                success=True
            )
        )
        



        

