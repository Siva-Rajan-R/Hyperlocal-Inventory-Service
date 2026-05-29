from pydantic import BaseModel
from typing import List,Optional
from infras.primary_db.services.inventory_service import InventoryService
from infras.primary_db.services.stock_adj_service import StockAdjService
from schemas.v1.request_schemas.stock_adj_schema import CreateStockAdjSchema,StockAdjustmentMovementType,StockAdjInventoryProductSchema,StockAdjustmentTypesEnum
from infras.primary_db.repos.inventory_repo import InventoryRepo
from infras.primary_db.main import AsyncInventoryLocalSession
from icecream import ic
from ..main import RabbitMQMessagingConfig
from datetime import date
from hyperlocal_platform.infras.saga.repo import SagaStatesRepo
from hyperlocal_platform.infras.saga.main import AsyncInfraDbLocalSession

class MessagingQueueBillingproducer:
    def __init__(self,headers:dict,payload:dict,saga_datas:dict):
        self.headers=headers
        self.payload=payload
        self.saga_datas=saga_datas

    async def process_stock_restore(self,orders_data:dict,billing_datas:dict,shop_id:Optional[str]=None,restore_type:str="EXCHANGE"):
        ic(orders_data,billing_datas,shop_id)
        if not shop_id:
            shop_id = billing_datas.get('shop_id')
            
        order_items_by_id = {}
        order_items_by_inventory_id = {}
        ic("Inside the stock recover function")
        for item in orders_data.get('items', []):
            ic(item)
            order_items_by_id[item['id']] = item
            order_items_by_inventory_id[item['inventory_id']] = item

        # Determine which items are being returned/refunded/exchanged to restore stock
        restore_items = []
        if 'exchange_items' in billing_datas:
            restore_items = billing_datas['exchange_items']
        elif 'items' in billing_datas:
            if isinstance(billing_datas['items'], list):
                if restore_type == "EXCHANGE":
                    restore_items = billing_datas.get('exchange_items', [])
                else:
                    restore_items = billing_datas['items']
            elif isinstance(billing_datas['items'], dict):
                # Single exchange - items key holds new item dict.
                # Old item to restore is identified by item_id.
                old_item_id = billing_datas.get('item_id')
                old_item = None
                for item in orders_data.get('items', []):
                    if item.get('id') == old_item_id:
                        old_item = item
                        break
                if old_item:
                    restore_items = [{
                        'id': old_item['id'],
                        'inventory_id': old_item['inventory_id'],
                        'quantity': old_item['quantity'],
                        'serial_numbers': billing_datas.get('inv_serial_numbers', []) or billing_datas.get('serial_numbers', [])
                    }]

        inventory_tocheck=[]
        inv_prod_toupdate:dict={}
        variant_toupdate:dict={}
        batch_toupdate:dict={}
        serialno_toupdate:dict={}
        stockadj_prod_toadd:List[StockAdjInventoryProductSchema]=[]
        
        ic(order_items_by_id)
        for billing_data in restore_items:
            ic(billing_data)
            item_id = billing_data.get('id') or billing_data.get('item_id')
            cur_inventory_data = None
            if item_id:
                cur_inventory_data = order_items_by_id.get(item_id)
            if not cur_inventory_data:
                cur_inventory_data = order_items_by_inventory_id.get(billing_data['inventory_id'])
                
            ic(cur_inventory_data)
            if not cur_inventory_data:
                ic("Inventory/order item not found in orders")
                return False
            
            inventory_id:str=billing_data['inventory_id']
            variant_id:str=cur_inventory_data.get('variant_id')
            batch_id:str=cur_inventory_data.get('batch_id')
            serialno_id:str=cur_inventory_data.get('serialno_id')
            serial_numbers=billing_data.get('serial_numbers') or billing_data.get('inv_serial_numbers') or []
            
            quantity_toincrease=billing_data['quantity']
            
            ic(inventory_id,variant_id,batch_id,serialno_id,serial_numbers,quantity_toincrease)

            inventory_tocheck.append(inventory_id)
            inv_prod_toupdate[inventory_id] = inv_prod_toupdate.get(inventory_id, 0) + quantity_toincrease

            if variant_id:
                variant_toupdate[variant_id] = variant_toupdate.get(variant_id, 0) + quantity_toincrease

            if batch_id:
                batch_toupdate[batch_id] = batch_toupdate.get(batch_id, 0) + quantity_toincrease

            if serialno_id and serial_numbers:
                serialno_toupdate[serialno_id] = serialno_toupdate.get(serialno_id, []) + serial_numbers

            stockadj_prod_toadd.append(
                StockAdjInventoryProductSchema(
                    inventory_id=inventory_id,
                    variant_id=variant_id,
                    batch_id=batch_id,
                    serialno_id=serialno_id,
                    serial_numbers=serial_numbers,
                    stocks=quantity_toincrease,
                    type=StockAdjustmentTypesEnum.INCREMENT
                )
            )

        ic(inventory_tocheck,inv_prod_toupdate,variant_toupdate,batch_toupdate,serialno_toupdate) 

        async with AsyncInventoryLocalSession() as session:
            inv_repo_obj=InventoryRepo(session=session)
            await inv_repo_obj.bulk_qty_update(data=inv_prod_toupdate,shop_id=shop_id)
            await inv_repo_obj.bulk_batch_qty_update(data=batch_toupdate,shop_id=shop_id)
            await inv_repo_obj.bulk_variant_qty_update(data=variant_toupdate,shop_id=shop_id)
            await inv_repo_obj.bulk_add_serialno(data=serialno_toupdate,shop_id=shop_id)

            await StockAdjService(session=session).create(
                can_update_stock=False,
                data=CreateStockAdjSchema(
                    shop_id=billing_datas['shop_id'],
                    adjusted_date=date.today(),
                    movement_type=restore_type.upper(),
                    description=f"Stock increased due to {restore_type.title()}",
                    products=stockadj_prod_toadd,
                )
            )
            
            ic("Stock Restored successfully")

        return True

    async def create_billing(self):
        orders_data={}
        try:
            ic(self.headers,self.payload,self.saga_datas)
            saga_datas:dict=self.saga_datas
            saga_id:str=self.headers.get('saga_id')

            # Re-fetch saga state from DB to avoid race conditions / stale cached data
            if saga_id:
                async with AsyncInfraDbLocalSession() as infra_session:
                    fresh_saga = await SagaStatesRepo(session=infra_session).getby_id(saga_id=saga_id)
                    if fresh_saga:
                        saga_datas = fresh_saga

            data:dict=saga_datas['data']

            billing_datas=data['billing']
            rb_msg=RabbitMQMessagingConfig(rabbitMQ_connection=await RabbitMQMessagingConfig.get_rabbitmq_connection())
            current_step = saga_datas['execution']['step']
            ic(current_step)
            if current_step=="ORDER_CREATION" or current_step=="ORDER_EXCHANGE":
                orders_data:dict|str=data.get('orders','not_found')

                if orders_data=="not_found":
                    return {"response":False,'execution':None}
                
                inv_stock_update={}
                variant_stock_update={}
                batch_stock_update={}
                serialno_update={}
                stockadj_prod_toadd:List[StockAdjInventoryProductSchema]=[]

                for product in billing_datas['products']:
                    # Accumulate quantities for same inventory/variant/batch
                    # (same product can appear with different variants or batches)
                    inv_stock_update[product['id']] = inv_stock_update.get(product['id'], 0) + product['quantity']

                    if product['variant_id']:
                        variant_stock_update[product['variant_id']] = variant_stock_update.get(product['variant_id'], 0) + product['quantity']

                    if product['batch_id']:
                        batch_stock_update[product['batch_id']] = batch_stock_update.get(product['batch_id'], 0) + product['quantity']

                    if product['serialno_id']:
                        existing = serialno_update.get(product['serialno_id'], [])
                        serialno_update[product['serialno_id']] = existing + product['serial_numbers']

                    # Keep individual line items for stock adjustment audit trail
                    stockadj_prod_toadd.append(
                        StockAdjInventoryProductSchema(
                            inventory_id=product['id'],
                            variant_id=product['variant_id'],
                            batch_id=product['batch_id'],
                            serialno_id=product['serialno_id'],
                            serial_numbers=product['serial_numbers'],
                            stocks=product['quantity'],
                            type=StockAdjustmentTypesEnum.INCREMENT
                        )
                    )
                    

                async with AsyncInventoryLocalSession() as session:
                    inv_obj=InventoryRepo(session=session)
                    ic(serialno_update)
                    ic(variant_stock_update)
                    ic(inv_stock_update)
                    ic(batch_stock_update)

                    await StockAdjService(session=session).create(
                        can_update_stock=False,
                        data=CreateStockAdjSchema(
                            shop_id=billing_datas['shop_id'],
                            adjusted_date=date.today(),
                            movement_type=StockAdjustmentMovementType.SALES,
                            description=f"Stock decreased due to Sales",
                            products=stockadj_prod_toadd,
                        )
                    )
                    
                    ic(serialno_update)
                    ic(variant_stock_update)
                    ic(inv_stock_update)
                    ic(batch_stock_update)
                    await inv_obj.bulk_update_serialno(data=serialno_update,shop_id=billing_datas['shop_id'])
                    await inv_obj.bulk_variant_decr_qty_update(shop_id=billing_datas['shop_id'],data=variant_stock_update)
                    await inv_obj.bulk_batch_decr_qty_update(shop_id=billing_datas['shop_id'],data=batch_stock_update)
                    await inv_obj.bulk_qty_decr_update(shop_id=billing_datas['shop_id'],data=inv_stock_update)
                    
                    

                    await session.commit()

                ic(orders_data)
                await rb_msg.publish_event(
                    routing_key="customers.service.routing.key",
                    exchange_name="customers.service.exchange",
                    payload=self.payload,
                    headers={
                        **self.headers,
                        'entity_name':'add_outstanding_customer',
                        'service_name':'CUSTOMERS',
                        'body':{
                            'id':billing_datas['customer_id'],
                            'shop_id':billing_datas['shop_id'],
                            'amount':billing_datas['payments'].get("CREDIT",0)
                        }
                    }
                )
                return {"response":True,'execution':{'next_step':'CUSTOMER_OUTSTANDING','service':'CUSTOMERS'}}
                
            if current_step=="CUSTOMER_OUTSTANDING":
                ic("CUSTOMER OUTSTANDING STEP ALREADY COMPLETED — ACK")
                return {"response":True,'execution':None}


            
            return {"response":False,'execution':None}
        

        except Exception as e:
            ic(f"A Unknown Exception Occur : {e}")
            if orders_data:
                await rb_msg.publish_event(
                    routing_key="orders.service.routing.key",
                    payload=self.payload,
                    headers={
                        **self.headers,
                        'entity_name':'delete_order',
                        'service_name':'ORDERS',
                        'body':{
                            'order_id':orders_data['id']
                        }
                    },
                    exchange_name="orders.service.exchange"
                )
        
                return {'response':True,'execution':{'next_step':'ORDERS_DELETE','service':'ORDERS'}}
            return {'response':False,'execution':None} 
        
    async def exchange_billing(self):
        orders_data={}
        try:
            ic(self.headers,self.payload,self.saga_datas)
            saga_datas:dict=self.saga_datas
            data:dict=saga_datas['data']

            billing_datas=data['billing']
            rb_msg=RabbitMQMessagingConfig(rabbitMQ_connection=await RabbitMQMessagingConfig.get_rabbitmq_connection())
            ic(saga_datas['execution']['step'])
            if saga_datas['execution']['step']=="ORDER_EXCHANGE":
                orders_data:dict|str=data.get('orders','not_found')

                if orders_data=="not_found":
                    return {"response":False,'execution':None}
                
                inv_stock_update={}
                variant_stock_update={}
                batch_stock_update={}
                serialno_update={}
                product=billing_datas['product']
                inv_stock_update[product['id']]=product['quantity']

                if product['variant_id']:
                    variant_stock_update[product['variant_id']]=product['quantity']

                if product['batch_id']:
                    batch_stock_update[product['batch_id']]=product['quantity']

                if product['serialno_id']:
                    serialno_update[product['serialno_id']]=product['serial_numbers']

                async with AsyncInventoryLocalSession() as session:
                    inv_obj=InventoryRepo(session=session)
                    ic(serialno_update)
                    ic(variant_stock_update)
                    ic(inv_stock_update)
                    ic(batch_stock_update)

                    
                    await inv_obj.bulk_update_serialno(data=serialno_update,shop_id=billing_datas['shop_id'])
                    await inv_obj.bulk_variant_decr_qty_update(shop_id=billing_datas['shop_id'],data=variant_stock_update)
                    await inv_obj.bulk_batch_decr_qty_update(shop_id=billing_datas['shop_id'],data=batch_stock_update)
                    await inv_obj.bulk_qty_decr_update(shop_id=billing_datas['shop_id'],data=inv_stock_update)
                
                await self.process_stock_restore(orders_data=orders_data,billing_datas=billing_datas)

                return {'response':True,'execution':{'next_step':'','service':''}}
            
            return {"response":False,'execution':None}
        

        except Exception as e:
            ic(f"A Unknown Exception Occur : {e}")
            if orders_data:
                await rb_msg.publish_event(
                    routing_key="orders.service.routing.key",
                    payload=self.payload,
                    headers={
                        **self.headers,
                        'entity_name':'delete_order',
                        'service_name':'ORDERS',
                        'body':{
                            'order_id':orders_data['id']
                        }
                    },
                    exchange_name="orders.service.exchange"
                )
        
                return {'response':True,'execution':{'next_step':'ORDERS_DELETE','service':'ORDERS'}}
            return {'response':False,'execution':None}
        

    async def return_billing(self):
        orders_data={}
        try:
            
            ic(self.headers,self.payload,self.saga_datas)
            saga_datas:dict=self.saga_datas
            data:dict=saga_datas['data']

            billing_datas=data['billing']
            orders_data=data['orders']

            shop_id:str=billing_datas['shop_id']
            ic(orders_data,billing_datas,shop_id)
            rb_msg=RabbitMQMessagingConfig(rabbitMQ_connection=await RabbitMQMessagingConfig.get_rabbitmq_connection())
            current_step=saga_datas['execution']['step']
            ic(current_step)

            if current_step=="ORDER_RETURN":
                res=await self.process_stock_restore(orders_data=orders_data,billing_datas=billing_datas,shop_id=shop_id,restore_type="RETURN")
                ic(res)
                if not res:
                    return {'response':False,'execution':'ORDER_RETURN'}
                
            return {'response':False,'execution':None}

                


        except Exception as e:
            ic("Error occured =>>",e)
         
    
    async def exchange_billing_bulk(self):
        orders_data={}
        try:
            ic(self.headers,self.payload,self.saga_datas)
            saga_datas:dict=self.saga_datas
            data:dict=saga_datas['data']

            billing_datas=data['billing']
            rb_msg=RabbitMQMessagingConfig(rabbitMQ_connection=await RabbitMQMessagingConfig.get_rabbitmq_connection())
            ic(saga_datas['execution']['step'])
            if saga_datas['execution']['step']=="ORDER_CREATION" or saga_datas['execution']['step']=="ORDER_EXCHANGE":
                orders_data:dict|str=data.get('orders','not_found')

                if orders_data=="not_found":
                    return {"response":False,'execution':None}
                
                inv_stock_update={}
                variant_stock_update={}
                batch_stock_update={}
                serialno_update={}

                for product in billing_datas['products']:
                    inv_stock_update[product['id']] = inv_stock_update.get(product['id'], 0) + product['quantity']

                    if product['variant_id']:
                        variant_stock_update[product['variant_id']] = variant_stock_update.get(product['variant_id'], 0) + product['quantity']

                    if product['batch_id']:
                        batch_stock_update[product['batch_id']] = batch_stock_update.get(product['batch_id'], 0) + product['quantity']

                    if product['serialno_id']:
                        existing = serialno_update.get(product['serialno_id'], [])
                        serialno_update[product['serialno_id']] = existing + product['serial_numbers']
                    

                async with AsyncInventoryLocalSession() as session:
                    inv_obj=InventoryRepo(session=session)
                    ic(serialno_update)
                    ic(variant_stock_update)
                    ic(inv_stock_update)
                    ic(batch_stock_update)

                    
                    await inv_obj.bulk_update_serialno(data=serialno_update,shop_id=billing_datas['shop_id'])
                    await inv_obj.bulk_variant_decr_qty_update(shop_id=billing_datas['shop_id'],data=variant_stock_update)
                    await inv_obj.bulk_batch_decr_qty_update(shop_id=billing_datas['shop_id'],data=batch_stock_update)
                    await inv_obj.bulk_qty_decr_update(shop_id=billing_datas['shop_id'],data=inv_stock_update)

                await self.process_stock_restore(orders_data=orders_data,billing_datas=billing_datas,shop_id=billing_datas['shop_id'],restore_type="EXCHANGE")

    
                ic("CUSTOMER DEDUCTED SUCCESSFULLY")
                ic(orders_data)
                await rb_msg.publish_event(
                    routing_key="customers.service.routing.key",
                    exchange_name="customers.service.exchange",
                    payload=self.payload,
                    headers={
                        **self.headers,
                        'entity_name':'add_outstanding_customer',
                        'service_name':'CUSTOMERS',
                        'body':{
                            'id':billing_datas['customer_id'],
                            'shop_id':billing_datas['shop_id'],
                            'amount':billing_datas['payments'].get("CREDIT",0)
                        }
                    }
                )
                return {"response":True,'execution':{'next_step':'CUSTOMER_OUTSTANDING','service':'CUSTOMERS'}}
                
            if saga_datas['execution']['step']=="CUSTOMER_OUTSTANDING":
                ic("CUSTOMER DEDUCTED SUCCESSFULLY")
                return {"response":False,'execution':None}


            
            return {"response":False,'execution':None}
        

        except Exception as e:
            ic(f"A Unknown Exception Occur : {e}")
            if orders_data:
                await rb_msg.publish_event(
                    routing_key="orders.service.routing.key",
                    payload=self.payload,
                    headers={
                        **self.headers,
                        'entity_name':'delete_order',
                        'service_name':'ORDERS',
                        'body':{
                            'order_id':orders_data['id']
                        }
                    },
                    exchange_name="orders.service.exchange"
                )
        
                return {'response':True,'execution':{'next_step':'ORDERS_DELETE','service':'ORDERS'}}
            return {'response':False,'execution':None} 