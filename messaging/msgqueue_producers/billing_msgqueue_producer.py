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

class MessagingQueueBillingproducer:
    def __init__(self,headers:dict,payload:dict,saga_datas:dict):
        self.headers=headers
        self.payload=payload
        self.saga_datas=saga_datas

    async def process_stock_restore(self,orders_data:dict,billing_datas:dict,shop_id:str,restore_type:str):
        ic(orders_data,billing_datas,shop_id)
        order_inventory_data={}
        ic("Inside the stock recover function")
        for item in orders_data['items']:
            ic(item)
            order_inventory_data[item['inventory_id']]=item

        inventory_tocheck=[]
        inv_prod_toupdate:dict={}
        variant_toupdate:dict={}
        batch_toupdate:dict={}
        serialno_toupdate:dict={}
        stockadj_prod_toadd:List[StockAdjInventoryProductSchema]=[]
        
        ic(order_inventory_data)
        for billing_data in billing_datas['items']:
            ic(billing_data)
            cur_inventory_data=order_inventory_data.get(billing_data['inventory_id'],None)
            ic(cur_inventory_data)
            if not cur_inventory_data:
                ic("Inventory id not found in orders")
                return False
            
            inventory_id:str=billing_data['inventory_id']
            variant_id:str=cur_inventory_data['variant_id']
            batch_id:str=cur_inventory_data['batch_id']
            serialno_id:str=cur_inventory_data['serialno_id']
            serial_numbers=billing_data['serial_numbers']
            
            quantity_toincrease=billing_data['quantity']
            
            ic(inventory_id,variant_id,batch_id,serialno_id,serial_numbers,quantity_toincrease)

            inventory_tocheck.append(inventory_id)
            inv_prod_toupdate[inventory_id]=quantity_toincrease

            if variant_id:
                variant_toupdate[variant_id]=quantity_toincrease

            if batch_id:
                batch_toupdate[batch_id]=quantity_toincrease

            if serialno_id:
                serialno_toupdate[serialno_id]=serial_numbers

            
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
                stockadj_prod_toadd:List[StockAdjInventoryProductSchema]=[]

                for product in billing_datas['products']:
                    inv_stock_update[product['id']]=product['quantity']

                    if product['variant_id']:
                        variant_stock_update[product['variant_id']]=product['quantity']

                    if product['batch_id']:
                        batch_stock_update[product['batch_id']]=product['quantity']

                    if product['serialno_id']:
                        serialno_update[product['serialno_id']]=product['serial_numbers']

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

                    
                    await inv_obj.bulk_update_serialno(data=serialno_update,shop_id=billing_datas['shop_id'])
                    await inv_obj.bulk_variant_decr_qty_update(shop_id=billing_datas['shop_id'],data=variant_stock_update)
                    await inv_obj.bulk_batch_decr_qty_update(shop_id=billing_datas['shop_id'],data=batch_stock_update)
                    await inv_obj.bulk_qty_decr_update(shop_id=billing_datas['shop_id'],data=inv_stock_update)
                    
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

                    await session.commit()

    
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