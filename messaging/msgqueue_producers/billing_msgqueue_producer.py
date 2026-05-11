from pydantic import BaseModel
from typing import List,Optional
from infras.primary_db.services.inventory_service import InventoryService
from infras.primary_db.repos.inventory_repo import InventoryRepo
from infras.primary_db.main import AsyncInventoryLocalSession
from icecream import ic
from ..main import RabbitMQMessagingConfig


class MessagingQueueBillingproducer:
    def __init__(self,headers:dict,payload:dict,saga_datas:dict):
        self.headers=headers
        self.payload=payload
        self.saga_datas=saga_datas

    async def create_billing(self):
        orders_data={}
        try:
            ic(self.headers,self.payload,self.saga_datas)
            saga_datas:dict=self.saga_datas
            data:dict=saga_datas['data']

            billing_datas=data['billing']
            rb_msg=RabbitMQMessagingConfig(rabbitMQ_connection=await RabbitMQMessagingConfig.get_rabbitmq_connection())
            ic(saga_datas['execution']['step'])
            if saga_datas['execution']['step']=="ORDER_CREATION":
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