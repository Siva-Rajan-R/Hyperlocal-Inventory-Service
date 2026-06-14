from schemas.v1.request_schemas.billing_schema import BillingProductSchema,CreateBillingSchema,CreateBillingReturnSchema,CreateBillingExchangeSchema,CreateBillingReturnBulkSchema,CreateBillingBulkExchangeSchema
from infras.primary_db.main import AsyncSession
from infras.primary_db.services.inventory_service import InventoryService,BulkCheckInventorySchema,GetAllInventorySchema,GetInventoryByIdSchema,GetInventoryByShopIdSchema
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,BaseResponseTypDict,ErrorResponseTypDict
from fastapi import HTTPException
from messaging.saga_producer import SagaProducer,CreateSagaStateSchema,SagaStatusEnum,SagaStateExecutionTypDict,SagaStateErrorTypDict
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from icecream import ic
from typing import Optional,List,Dict
from infras.primary_db.repos.inventory_repo import InventoryRepo
from schemas.v1.request_schemas.stock_adj_schema import CreateStockAdjOnlySchema,StockAdjInventoryProductOnlySchema
from core.data_formats.enums.stock_adj_enums import StockAdjustmentMovementType,StockAdjustmentTypesEnum
from infras.primary_db.services.stock_adj_service import StockAdjService

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
    

    async def create_v2(self,data:CreateBillingSchema):
        if not data.customer_id and data.payments.get('CREDIT', 0) > 0:
            self.raise_bad_request("Walk-in customers cannot have credit payments. Full amount must be paid.")

        inv_repo_obj=InventoryRepo(session=self.session)
        order_items=[]
        structured_data:Dict[str,List[dict]]={}
        inventory_tocheck,variant_tocheck,batch_tocheck,serialno_tocheck=[],[],[],[]


        # STEP-1 DUPLICATION IDENTIFICATION
        for product in data.products:
            ERROR=None
            if product.id not in structured_data:
                structured_data[product.id]=[]
            else:
                values=structured_data[product.id]
                for val in values:
                    variant_id,batch_id,serialno_id=val['variant_id'],val['batch_id'],val['serialno_id']
                    inc_variant_id,inc_batch_id,inc_serialno_id=product.variant_id,product.batch_id,product.serialno_id

                    if variant_id==inc_variant_id and batch_id==inc_batch_id:
                        ic("A same product or variant or batch appeared")
                        ERROR=True
                        break

            if ERROR:
                ic("Error occured")
                return False
        
            structured_data[product.id].append(product.model_dump())
            if product.id not in inventory_tocheck:
                inventory_tocheck.append(product.id)
            if product.variant_id and product.variant_id not in variant_tocheck:
                variant_tocheck.append(product.variant_id)
            if product.batch_id and product.batch_id not in batch_tocheck:
                batch_tocheck.append(product.batch_id)
            if product.serialno_id and product.serialno_id not in serialno_tocheck:
                serialno_tocheck.append(product.serialno_id)

            
        
        

        ic(inventory_tocheck,variant_tocheck,batch_tocheck,serialno_tocheck)

        # STEP-2 DB CHECK
        inv_checked_results=await inv_repo_obj.bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inventory_tocheck))
        variant_checked_results=await inv_repo_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=variant_tocheck)
        batch_checked_results=await inv_repo_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=batch_tocheck)
        serialno_checked_results=await inv_repo_obj.bulk_serialno_check(shop_id=data.shop_id,serialnos_id=serialno_tocheck)

        if len(inventory_tocheck)!=len(inv_checked_results) or len(variant_tocheck)!=len(variant_checked_results) or len(batch_tocheck)!=len(batch_checked_results) or len(serialno_tocheck)!=len(serialno_checked_results):
            ic("Inventory,batch,variant,seriano some of these id was mistmatched")
            return False
        

        structured_inv_result_data,structured_variant_result_data,structured_batch_result_data={},{},{}
        for result in inv_checked_results:
            structured_inv_result_data[result['id']]=result

        for result in variant_checked_results:
            structured_variant_result_data[result['id']]=result

        for result in batch_checked_results:
            structured_batch_result_data[result['id']]=result
        
        ic(structured_inv_result_data,structured_variant_result_data,structured_batch_result_data)


        inv_prod_toupdate:dict={}
        variant_toupdate:dict={}
        batch_toupdate:dict={}
        serialno_toupdate:dict={}

        stock_adj_products=[]

        for key,value in structured_data.items():
            ERROR=None
            inventory_id=key
            inv_stocks=0
            for val in value:
                has_variant=structured_inv_result_data[inventory_id]['has_variant']
                has_batch=structured_inv_result_data[inventory_id]['has_batch']
                has_serialno=structured_inv_result_data[inventory_id]['has_serialno']
                

                variant_id,batch_id,serialno_id=val['variant_id'],val['batch_id'],val['serialno_id']

                ic(val)
                stocks:int=val['quantity']
                variant_id:str=val.get('variant_id',None)
                batch_id:str=val.get('batch_id')
                serialno_id:str=val.get('serialno_id')
                serial_numbers:list[str]=val.get('serial_numbers',None)


                inv_stocks_before=structured_inv_result_data[inventory_id]['stocks'] if inventory_id in structured_inv_result_data else 0.0
                batch_stocks_before=structured_batch_result_data[batch_id]['stocks'] if batch_id in structured_batch_result_data else 0.0
                variant_stocks_before=structured_variant_result_data[variant_id]['stocks'] if variant_id in structured_variant_result_data else 0.0
                

                if has_variant and not variant_id:
                    ic("Variant id does not exists")
                    ERROR=True
                    break
                    
                if has_batch and not batch_id:
                    ic("batch id doesnot exists")
                    ERROR=True
                    break

                if has_serialno and (not serialno_id or len(serial_numbers or []) != stocks):
                    ic("Serialno doesnot exists or serial numbers does not match the quantity")
                    ERROR=True
                    break

                if variant_id:
                    variant_toupdate[variant_id] = variant_toupdate.get(variant_id, 0) + stocks

                if batch_id:
                    batch_toupdate[batch_id] = batch_toupdate.get(batch_id, 0) + stocks


                if serialno_id and serial_numbers:
                    serialno_toupdate.setdefault(serialno_id, []).extend(serial_numbers)


                stocks_before=inv_stocks_before
                if has_variant:
                    stocks_before=variant_stocks_before
                if has_batch:
                    stocks_before=batch_stocks_before
                
                if has_variant and has_batch:
                    stocks_before=batch_stocks_before

                stock_adj_products.append(
                    StockAdjInventoryProductOnlySchema(
                        inventory_id=inventory_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        serialno_id=serialno_id,
                        serial_numbers=serial_numbers,
                        stocks=stocks,
                        stocks_before=stocks_before,
                        type=StockAdjustmentTypesEnum.DECREMENT
                    )
                )

                inv_stocks+=stocks

                buy_price=structured_inv_result_data[inventory_id]['buy_price']
                sell_price=structured_inv_result_data[inventory_id]['sell_price']

                if variant_id:
                    buy_price=structured_variant_result_data[variant_id]['buy_price']
                    sell_price=structured_variant_result_data[variant_id]['sell_price']


                item_datas = {
                    "product_name": structured_inv_result_data[inventory_id]['name'],
                    "gst": structured_inv_result_data[inventory_id]['datas'].get('gst', '0%'),
                    "category": structured_inv_result_data[inventory_id].get('category', "Uncategorized"),
                    "supplier": structured_inv_result_data[inventory_id].get('datas', {}).get('supplier', "Unknown")
                }
                if variant_id and variant_id in structured_variant_result_data:
                    item_datas["variant_name"] = structured_variant_result_data[variant_id]['name']
                if batch_id and batch_id in structured_batch_result_data:
                    item_datas["batch_name"] = structured_batch_result_data[batch_id]['name']
                    if structured_batch_result_data[batch_id].get('mfg_date'):
                        item_datas["mfg_date"] = str(structured_batch_result_data[batch_id]['mfg_date'])
                    if structured_batch_result_data[batch_id].get('exp_date'):
                        item_datas["exp_date"] = str(structured_batch_result_data[batch_id]['exp_date'])

                order_items.append(
                    {
                        'inventory_id':inventory_id,
                        'variant_id':variant_id,
                        'batch_id':batch_id,
                        'serialno_id':serialno_id,
                        'barcode':structured_inv_result_data[inventory_id]['barcode'],
                        'inv_serial_numbers':serial_numbers,
                        'buy_price':buy_price,
                        'sell_price':sell_price,
                        'gst':structured_inv_result_data[inventory_id]['datas']['gst'],
                        'quantity':val['quantity'],
                        'datas': item_datas
                    }
                )

            
            inv_prod_toupdate[inventory_id] = inv_stocks

            
        
        ic(serialno_toupdate)
        ic(variant_toupdate)
        ic(inv_prod_toupdate)
        ic(batch_toupdate)
        await inv_repo_obj.bulk_update_serialno(data=serialno_toupdate,shop_id=data.shop_id)
        await inv_repo_obj.bulk_variant_decr_qty_update(shop_id=data.shop_id,data=variant_toupdate)
        await inv_repo_obj.bulk_batch_decr_qty_update(shop_id=data.shop_id,data=batch_toupdate)
        await inv_repo_obj.bulk_qty_decr_update(shop_id=data.shop_id,data=inv_prod_toupdate)
        from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
        from infras.read_db.models.inventory_model import InventoryReadModel
        from schemas.v1.request_schemas.inventory_schema import GetInventoryByIdSchema
        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id

        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        inv_config = shop_config.get("product", {})
        prefix = inv_config.get("prefix", "PROD")
        start_from = inv_config.get("start_from", 1)
        
        for inv_id in inv_prod_toupdate.keys():
            raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inv_id))
            if raw_inventory:
                raw_dict = dict(raw_inventory)
                await InventoryReadDbRepo.replace_inventory(InventoryReadModel(**raw_dict))


        await StockAdjService(session=self.session).make_stock_adjustment(
            data=CreateStockAdjOnlySchema(
                shop_id=data.shop_id,
                movement_type=StockAdjustmentMovementType.SALES,
                description=f"Stock decreased due to Sales",
                products=stock_adj_products,
            )
        )


        exchange_name="orders.service.exchange"
        r_key="orders.serivce.routing.key"
        reply_key='billing.producer.routing.key'
        reply_exchange='billing.producer.exchange'
        reply_service_name="BILLING"
        reply_entity_name="create_billing_v2"
        
        data_toadd=data.model_dump(mode='json')
        saga_id:str=generate_uuid()
        ic(data.payments)
        body={
            'shop_id':data.shop_id,
            'customer_id':data.customer_id,
            'status':data.status,
            'origin':'OFFLINE',
            'payments':data.payments,
            'items':order_items
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
        

    async def create(self,data:CreateBillingSchema):
        if not data.customer_id and data.payments.get('CREDIT', 0) > 0:
            self.raise_bad_request("Walk-in customers cannot have credit payments. Full amount must be paid.")
            
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
        
        db_batches = {batch['id']: batch for batch in batch_res}
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
            item_datas = {
                "product_name": db_products[val['id']]['name'],
                "gst": db_products[val['id']]['datas'].get('gst', '0%'),
                "category": db_products[val['id']].get('category', "Uncategorized"),
                "supplier": db_products[val['id']].get('datas', {}).get('supplier', "Unknown")
            }
            if val.get('variant_id') and val['variant_id'] in db_variants:
                item_datas["variant_name"] = db_variants[val['variant_id']]['name']
            if val.get('batch_id') and val['batch_id'] in db_batches:
                batch_info = db_batches[val['batch_id']]
                item_datas["batch_name"] = batch_info['name']
                if batch_info.get('mfg_date'):
                    item_datas["mfg_date"] = str(batch_info['mfg_date'])
                if batch_info.get('exp_date'):
                    item_datas["exp_date"] = str(batch_info['exp_date'])

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
                    'datas': item_datas
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
            'item_id':data.item_id,
            'shop_id':data.shop_id,
            'customer_id':data.customer_id,
            'payments':data.payments
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
            'customer_id':data.customer_id,
            'payments':data.payments,
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


        item_datas = {
            "product_name": product['name'],
            "gst": product['datas'].get('gst', '0%'),
            "category": product.get('category', "Uncategorized"),
            "supplier": product.get('datas', {}).get('supplier', "Unknown")
        }
        if product['has_variant'] and 'variant_exists' in locals() and variant_exists:
            item_datas["variant_name"] = variant_exists[0]['name']
        if product['has_batch'] and 'batch_exists' in locals() and batch_exists:
            item_datas["batch_name"] = batch_exists[0]['name']
            if batch_exists[0].get('mfg_date'):
                item_datas["mfg_date"] = str(batch_exists[0]['mfg_date'])
            if batch_exists[0].get('exp_date'):
                item_datas["exp_date"] = str(batch_exists[0]['exp_date'])

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
            'quantity':data.product.quantity,
            'datas': item_datas
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
            'payments':data.payments,
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
        
        db_batches = {batch['id']: batch for batch in batch_res}
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
            item_datas = {
                "product_name": db_products[val['id']]['name'],
                "gst": db_products[val['id']]['datas'].get('gst', '0%')
            }
            if val.get('variant_id') and val['variant_id'] in db_variants:
                item_datas["variant_name"] = db_variants[val['variant_id']]['name']
            if val.get('batch_id') and val['batch_id'] in db_batches:
                batch_info = db_batches[val['batch_id']]
                item_datas["batch_name"] = batch_info['name']
                if batch_info.get('mfg_date'):
                    item_datas["mfg_date"] = str(batch_info['mfg_date'])
                if batch_info.get('exp_date'):
                    item_datas["exp_date"] = str(batch_info['exp_date'])

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
                    'datas': item_datas
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

    async def get_billing_stats(self, shop_id: str):
        from infras.read_db.repos.billing_repo import BillingStatsReadDbRepo
        stats = await BillingStatsReadDbRepo.get_stats(shop_id=shop_id)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Billing stats fetched successfully",
                status_code=200,
                success=True
            ),
            data=stats
        )

    async def get_billings(self, shop_id: str, limit: int = 50, skip: int = 0):
        from infras.read_db.repos.billing_repo import BillingReadDbRepo
        billings = await BillingReadDbRepo.get_billings_by_shop(shop_id=shop_id, limit=limit, skip=skip)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Billings fetched successfully",
                status_code=200,
                success=True
            ),
            data={'billings': billings}
        )



        

