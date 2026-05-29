from schemas.v1.request_schemas.billing_schema import BillingProductSchema,CreateBillingSchema,CreateBillingReturnSchema,CreateBillingExchangeSchema,CreateBillingReturnBulkSchema,CreateBillingBulkExchangeSchema
from infras.primary_db.main import AsyncSession
from infras.primary_db.services.inventory_service import InventoryService,BulkCheckInventorySchema,GetAllInventorySchema,GetInventoryByIdSchema,GetInventoryByShopIdSchema
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,BaseResponseTypDict,ErrorResponseTypDict
from fastapi import HTTPException
from messaging.saga_producer import SagaProducer,CreateSagaStateSchema,SagaStatusEnum,SagaStateExecutionTypDict,SagaStateErrorTypDict
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from icecream import ic

class HandleBillingRequest:
    def __init__(self,session:AsyncSession):
        self.session=session


    def raise_bad_request(self,description: str, msg: str = "Error Creating Billing"):
        raise HTTPException(
            status_code=400,
            detail=ErrorResponseTypDict(
                msg=msg,
                success=False,
                description=description,
                status_code=400
            )
        )

    async def create(self,data:CreateBillingSchema):
        
        inv_service_obj=InventoryService(session=self.session)
        unique_inv_ids = list(set(product.id for product in data.products))
        items=[]
        ERROR_OCCURED=None

        res=await inv_service_obj.bulk_check(
            data=BulkCheckInventorySchema(shop_id=data.shop_id,id=unique_inv_ids)
        )

        if len(unique_inv_ids)!=len(res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Products",
                    status_code=400
                )
            )
        
        db_products = {inv['id']: inv for inv in res}
        processed_products = []
        variant_id=[]
        batch_id=[]
        serialno_id=[]

        for product in data.products:
            inv_res = db_products.get(product.id)
            if not inv_res:
                ERROR_OCCURED=True
                break

            prod_dict = product.model_dump()
            ic(inv_res)
            ic(prod_dict)

            if inv_res['has_variant'] and not prod_dict['variant_id']:
                ERROR_OCCURED=True
                ic("Variant exists but variant id not found")
                break 

            if inv_res['has_batch'] and not prod_dict['batch_id']:
                ERROR_OCCURED=True
                ic("batch exists but batch id not found")
                break
            
            ic(inv_res['has_serialno'])
            ic(inv_res['has_serialno'] and (not prod_dict['serialno_id'] or len(prod_dict['serial_numbers'] or [])!=prod_dict['quantity']))
            ic(inv_res['has_serialno'],not prod_dict['serialno_id'],len(prod_dict['serial_numbers'] or [])!=prod_dict['quantity'])
            if inv_res['has_serialno'] and (not prod_dict['serialno_id'] or len(prod_dict['serial_numbers'] or [])!=prod_dict['quantity']):
                ERROR_OCCURED=True
                ic("Serial no exists but serialno id not found or serial numbers does not match the quantity")
                break

            if inv_res['has_variant']:
                variant_id.append(prod_dict['variant_id'])

            if inv_res['has_batch']:
                batch_id.append(prod_dict['batch_id'])

            if inv_res['has_serialno']:
                serialno_id.append(prod_dict['serialno_id'])

            prod_dict['buy_price']=inv_res['buy_price']
            prod_dict['sell_price']=inv_res['sell_price']
            prod_dict['barcode']=inv_res['barcode']
            prod_dict['gst']="0%"

            processed_products.append(prod_dict)

        if ERROR_OCCURED:
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Variants,batch,serial or Invalid serial numbers",
                    status_code=400
                )
            )

        unique_variant_ids = list(set(variant_id))
        variant_res=await inv_service_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=unique_variant_ids)
        if len(unique_variant_ids)!=len(variant_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Variants",
                    status_code=400
                )
            )
        
        db_variants = {variant['id']: variant for variant in variant_res}
        for prod in processed_products:
            if prod['variant_id']:
                variant = db_variants.get(prod['variant_id'])
                if variant:
                    prod['buy_price']=variant['buy_price']
                    prod['sell_price']=variant['sell_price']
        
        unique_batch_ids = list(set(batch_id))
        batch_res=await inv_service_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=unique_batch_ids)
        if len(unique_batch_ids)!=len(batch_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Batch",
                    status_code=400
                )
            )
        
        unique_serialno_ids = list(set(serialno_id))
        serialno_res=await inv_service_obj.bulk_serialno_check(shop_id=data.shop_id,serianos_id=unique_serialno_ids)
        if len(unique_serialno_ids)!=len(serialno_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Batch",
                    status_code=400
                )
            )
        

        for val in processed_products:
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
        ic(data.payments)
        body={
            'shop_id':data.shop_id,
            'customer_id':data.customer_id,
            'status':data.status,
            'origin':'OFFLINE',
            'payments':data.payments,
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
                    step="ORDER_CREATION",service="ORDERS"
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
    


    async def return_order(self,data:CreateBillingReturnSchema):
        exchange_name="orders.service.exchange"
        r_key="orders.serivce.routing.key"
        reply_key='billing.producer.routing.key'
        reply_exchange='billing.producer.exchange'
        reply_service_name="BILLING"
        reply_entity_name="return_billing"
        
        data_toadd=data.model_dump(mode='json')
        saga_id:str=generate_uuid()

        body={
            'id':data.order_id,
            'item_id':data.item_id
        }

        headers={
            "reply_key":reply_key,
            "reply_exchange":reply_exchange,
            "reply_service_name":reply_service_name,
            "reply_entity_name":reply_entity_name,
            "service_name":"ORDERS",
            "entity_name":"return_order",
            "body":body

        }
        await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.IN_PROGRESS,
                type="SERVICE",
                data={'billing':data_toadd},
                steps={
                    'ORDER_RETURN':'PENDING'
                },
                execution=SagaStateExecutionTypDict(
                    step="ORDER_RETURN",service="ORDERS"
                )
            ),
            headers=headers,
            exchange_name=exchange_name,
            routing_key=r_key
        )

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Billing Returned Successfully",
                status_code=203,
                success=True
            )
        )
    

    async def return_order_bulk(self,data:CreateBillingReturnBulkSchema):
        exchange_name="orders.service.exchange"
        r_key="orders.serivce.routing.key"
        reply_key='billing.producer.routing.key'
        reply_exchange='billing.producer.exchange'
        reply_service_name="BILLING"
        reply_entity_name="return_billing"
        
        data_toadd=data.model_dump(mode='json')
        saga_id:str=generate_uuid()

        body={
            'id':data.order_id,
            'shop_id':data.shop_id,
            'items':[{'id':item.item_id,'quantity':item.quantity,'reason':item.reason} for item in data.items]
        }

        ic(body)

        headers={
            "reply_key":reply_key,
            "reply_exchange":reply_exchange,
            "reply_service_name":reply_service_name,
            "reply_entity_name":reply_entity_name,
            "service_name":"ORDERS",
            "entity_name":"return_order_bulk",
            "body":body

        }
        await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.IN_PROGRESS,
                type="SERVICE",
                data={'billing':data_toadd},
                steps={
                    'ORDER_RETURN':'PENDING'
                },
                execution=SagaStateExecutionTypDict(
                    step="ORDER_RETURN",service="ORDERS"
                )
            ),
            headers=headers,
            exchange_name=exchange_name,
            routing_key=r_key
        )

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Billing Returned Successfully",
                status_code=203,
                success=True
            )
        )
    


    async def exchange_order(self,data:CreateBillingExchangeSchema):
        inv_service_obj=InventoryService(session=self.session)
        product=await inv_service_obj.getby_id(data=GetInventoryByIdSchema(shop_id=data.shop_id,id=data.product.id))
        ic(product)
        if not product:
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Products",
                    status_code=400
                )
            )
        
        
        buy_price:float=product['buy_price']
        sell_price:float=product['sell_price']
        gst:str="0%"
        barcode:str=product['barcode']

        ERROR_OCCURED=None
        
        if product['has_variant'] and not data.product.variant_id:
            ERROR_OCCURED=True
            ic("Variant exists Variant id not found")
            self.raise_bad_request("Variant exists Variant id not found")
            
        if product['has_batch'] and not data.product.batch_id:
            ERROR_OCCURED=True
            ic("Batch Exists Batch Id not found")
            self.raise_bad_request("Batch Exists Batch Id not found")

        if product['has_serialno'] and (not data.product.serialno_id or len(data.product.serial_numbers or [])!=data.product.quantity):
            ERROR_OCCURED=True
            ic("serial no exists Serialno id not found or serial no not matched to the given quantity")
            self.raise_bad_request("serial no exists Serialno id not found or serial no not matched to the given quantity")

        if product['has_variant']:
            variant_exists=await inv_service_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=[data.product.variant_id])
            ic(variant_exists)
            if not variant_exists:
                ERROR_OCCURED=True
                ic("Invalid Variant id")
                self.raise_bad_request("Invalid Variant id")
            
            buy_price=variant_exists[0]['buy_price']
            sell_price=variant_exists[0]['sell_price']
            

        if product['has_batch']:
            batch_exists=await inv_service_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=[data.product.batch_id])
            ic(batch_exists)
            if not batch_exists:

                ERROR_OCCURED=True
                ic("Invalid Batch id")
                self.raise_bad_request("Invalid Batch id")

        if product['has_serialno']:
            serialno_exists=await inv_service_obj.bulk_serialno_check(shop_id=data.shop_id,serianos_id=[data.product.serialno_id])
            ic(serialno_exists)
            if not serialno_exists:
                ERROR_OCCURED=True
                ic("invalid Serial no id")
                self.raise_bad_request("invalid Serial no id")
            

        
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


        item={
            'inventory_id':data.product.id,
            'variant_id':data.product.variant_id,
            'batch_id':data.product.batch_id,
            'serialno_id':data.product.serialno_id,
            'barcode':barcode,
            'inv_serial_numbers':data.product.serial_numbers,
            'buy_price':buy_price,
            'sell_price':sell_price,
            'gst':gst,
            'quantity':data.product.quantity
        }
        
        exchange_name="orders.service.exchange"
        r_key="orders.serivce.routing.key"
        reply_key='billing.producer.routing.key'
        reply_exchange='billing.producer.exchange'
        reply_service_name="BILLING"
        reply_entity_name="exchange_billing"
        
        data_toadd=data.model_dump(mode='json')
        saga_id:str=generate_uuid()

        body={
            'shop_id':data.shop_id,
            'customer_id':data.customer_id,
            'order_id':data.order_id,
            'item_id':data.item_id,
            'payment_method':data.payment_method,
            'items':item
        }

        headers={
            "reply_key":reply_key,
            "reply_exchange":reply_exchange,
            "reply_service_name":reply_service_name,
            "reply_entity_name":reply_entity_name,
            "service_name":"ORDERS",
            "entity_name":"exchange_order",
            "body":body

        }
        await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.IN_PROGRESS,
                type="SERVICE",
                data={'billing':data_toadd},
                steps={
                    'ORDER_EXCHANGE':'PENDING'
                },
                execution=SagaStateExecutionTypDict(
                    step="ORDER_EXCHANGE",service="ORDERS"
                )
            ),
            headers=headers,
            exchange_name=exchange_name,
            routing_key=r_key
        )

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Billing Exchanged Successfully",
                status_code=203,
                success=True
            )
        )
    

    async def exchange_order_bulk(self,data:CreateBillingBulkExchangeSchema):
        inv_service_obj=InventoryService(session=self.session)
        unique_inv_ids = list(set(product.id for product in data.products))
        items=[]
        ERROR_OCCURED=None

        res=await inv_service_obj.bulk_check(
            data=BulkCheckInventorySchema(shop_id=data.shop_id,id=unique_inv_ids)
        )

        if len(unique_inv_ids)!=len(res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Products",
                    status_code=400
                )
            )
        
        db_products = {inv['id']: inv for inv in res}
        processed_products = []
        variant_id=[]
        batch_id=[]
        serialno_id=[]

        for product in data.products:
            inv_res = db_products.get(product.id)
            if not inv_res:
                ERROR_OCCURED=True
                break

            prod_dict = product.model_dump()
            ic(inv_res)
            ic(prod_dict)

            if inv_res['has_variant'] and not prod_dict['variant_id']:
                ERROR_OCCURED=True
                ic("Variant exists but variant id not found")
                break 

            if inv_res['has_batch'] and not prod_dict['batch_id']:
                ERROR_OCCURED=True
                ic("batch exists but batch id not found")
                break
            
            ic(inv_res['has_serialno'])
            ic(inv_res['has_serialno'] and (not prod_dict['serialno_id'] or len(prod_dict['serial_numbers'] or [])!=prod_dict['quantity']))
            ic(inv_res['has_serialno'],not prod_dict['serialno_id'],len(prod_dict['serial_numbers'] or [])!=prod_dict['quantity'])
            if inv_res['has_serialno'] and (not prod_dict['serialno_id'] or len(prod_dict['serial_numbers'] or [])!=prod_dict['quantity']):
                ERROR_OCCURED=True
                ic("Serial no exists but serialno id not found or serial numbers does not match the quantity")
                break

            if inv_res['has_variant']:
                variant_id.append(prod_dict['variant_id'])

            if inv_res['has_batch']:
                batch_id.append(prod_dict['batch_id'])

            if inv_res['has_serialno']:
                serialno_id.append(prod_dict['serialno_id'])

            prod_dict['buy_price']=inv_res['buy_price']
            prod_dict['sell_price']=inv_res['sell_price']
            prod_dict['barcode']=inv_res['barcode']
            prod_dict['gst']="0%"

            processed_products.append(prod_dict)

        if ERROR_OCCURED:
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Variants,batch,serial or Invalid serial numbers",
                    status_code=400
                )
            )

        unique_variant_ids = list(set(variant_id))
        variant_res=await inv_service_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=unique_variant_ids)
        if len(unique_variant_ids)!=len(variant_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Variants",
                    status_code=400
                )
            )
        
        db_variants = {variant['id']: variant for variant in variant_res}
        for prod in processed_products:
            if prod['variant_id']:
                variant = db_variants.get(prod['variant_id'])
                if variant:
                    prod['buy_price']=variant['buy_price']
                    prod['sell_price']=variant['sell_price']
        
        unique_batch_ids = list(set(batch_id))
        batch_res=await inv_service_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=unique_batch_ids)
        if len(unique_batch_ids)!=len(batch_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Batch",
                    status_code=400
                )
            )
        
        unique_serialno_ids = list(set(serialno_id))
        serialno_res=await inv_service_obj.bulk_serialno_check(shop_id=data.shop_id,serianos_id=unique_serialno_ids)
        if len(unique_serialno_ids)!=len(serialno_res):
            return HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error Creating Billing",
                    success=False,
                    description="invalid Batch",
                    status_code=400
                )
            )
        

        for val in processed_products:
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
                    'quantity':val['quantity'],
                    'datas':val['datas']
                }
            )
        
        exchange_name="orders.service.exchange"
        r_key="orders.serivce.routing.key"
        reply_key='billing.producer.routing.key'
        reply_exchange='billing.producer.exchange'
        reply_service_name="BILLING"
        reply_entity_name="exchange_billing_bulk"
        
        data_toadd=data.model_dump(mode='json')
        saga_id:str=generate_uuid()

        body={
            'shop_id':data.shop_id,
            'customer_id':data.customer_id,
            'payments':data.payments,
            'items':items,
            'order_id':data.order_id,
            'exchange_items':[{'id':item.item_id,'quantity':item.quantity,'reason':item.reason} for item in data.items],
        }

        headers={
            "reply_key":reply_key,
            "reply_exchange":reply_exchange,
            "reply_service_name":reply_service_name,
            "reply_entity_name":reply_entity_name,
            "service_name":"ORDERS",
            "entity_name":"exchange_order_bulk",
            "body":body

        }
        await SagaProducer.emit(
            saga_payload=CreateSagaStateSchema(
                id=saga_id,
                status=SagaStatusEnum.IN_PROGRESS,
                type="SERVICE",
                data={'billing':data_toadd},
                steps={
                    'ORDER_EXCHANGE':'PENDING'
                },
                execution=SagaStateExecutionTypDict(
                    step="ORDER_EXCHANGE",service="ORDERS"
                )
            ),
            headers=headers,
            exchange_name=exchange_name,
            routing_key=r_key
        )

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Billing Exchanged Successfully",
                status_code=203,
                success=True
            )
        )



        

