from typing import Optional,List
from pydantic import BaseModel
from messaging.saga_producer import SagaProducer,SagaStateErrorTypDict,SagaStateExecutionTypDict,CreateSagaStateSchema
from infras.primary_db.repos.product_repo import ProductRepo
from infras.primary_db.main import AsyncSession
from icecream import ic
from datetime import datetime
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from hyperlocal_platform.core.enums.saga_state_enum import SagaStatusEnum,SagaStepsValueEnum
from schemas.v1.product_schemas.request_schemas import GetBulkProductsById




async def emit_stock_mov_adj(session:AsyncSession,data: List[dict]):
    """
    The data need to be should like 
    {
        'product_id':product_id,
        'variant_id':variant_id,
        'batch_id':batch_id,
        'serial_numbers':serialno_infos,
        'type':update_type,
        'stocks':stocks,
        'shop_id':shop_id,
        'entity_name':itm['entity_name']
    }

    ENTITY_NAME => SERVICE NAME
    UPDATE_TYPE => [INCREMENT,DECREMENT]
    """

    print("Inside emit stock mov adj")
    prod_repo_obj=ProductRepo(session=session)

    validated_data={}
    product_ids=[]
    shop_id:str=None
    
    ic(data)

    for prod in data:
        ic(prod)
        product_id=prod['product_id']
        shop_id=prod['shop_id']
        if product_id not in validated_data:
            validated_data[product_id]=[]
        
        validated_data[product_id].append(prod)
        if product_id not in product_ids:
            product_ids.append(product_id)
    
    ic(validated_data)

    product_res=await prod_repo_obj.get_bulk_products_by_id(data=GetBulkProductsById(shop_id=shop_id,id=product_ids))
    ic(product_res)
    stock_mov_adj_items=[]
    adj_date=datetime.now()
    entity_name=""
    for prod in product_res:
        ic(prod)
        product_id=prod['id']
        product_name=prod['name']
        ui_id=prod['ui_id']
        has_variant=prod['type_infos']['has_variant']
        has_batch=prod['type_infos']['has_batch']
        has_serialno=prod['type_infos']['has_serialno']
        variant_id=None
        batch_id=None
        variant_name=""
        batch_infos={}
        serialno_infos=[]
        stock_infos={}
        stl_infos={}
        rop_infos={}
        update_type=None

        stock_before=0
        stock_after=0
        stocks=0


        strut_prod_res=validated_data.get(product_id)
        ic(strut_prod_res)
        for val in strut_prod_res:
            ic(val)
            batch_id=val['batch_id']
            variant_id=val['variant_id']
            if has_variant:
                if variant_id in prod['variants']:
                    stock_infos=prod['variants'][variant_id].get("stock_infos",{})
            
                    variant_name=prod['variants']['name']
                    if has_batch and not has_serialno:
                        batch_infos=prod['variants'][variant_id]['batch_infos'].get("batch_id",{})
                        stock_infos=batch_infos['stock_infos']
                    
                    if has_serialno and not has_batch:
                        serialno_infos=prod['variants'][variant_id].get('serialno_infos',{})

                    if has_serialno and has_batch:
                        batch_infos=prod['variants'][variant_id]['batch_infos'].get("batch_id",{})
                        stock_infos=batch_infos['stock_infos']
                        serialno_infos=batch_infos['serialno_infos']


                    stocks=val['stocks']
                    stock_before=stock_infos['physical_stocks']-stocks if val['type']=="INCREMENT" else stock_infos['physical_stocks']+stocks
                    stock_after=stock_infos['physical_stocks']



            else:

                    stock_infos=stock_infos=prod.get("stock_infos",{})

                    if has_batch and not serialno_infos:
                        batch_infos=prod['batch_infos'][batch_id]
                        stock_infos=batch_infos['stock_infos']
                    
                    if has_serialno and not has_batch:
                        serialno_infos=prod['serialno_infos']
                    
                    if has_serialno and has_batch:
                        batch_infos=prod['batch_infos'][batch_id]
                        stock_infos=batch_infos['stock_infos']
                        serialno_infos=batch_infos['serialno_infos']

                    stocks=val['stocks']
                    stock_before=stock_infos['physical_stocks']-stocks if val['type']=="INCREMENT" else stock_infos['physical_stocks']+stocks
                    stock_after=stock_infos['physical_stocks']

            update_type=val['type']
            entity_name=val['entity_name']

            stock_mov_adj_items.append(
                {
                    'product_id':product_id,
                    'name':product_name,
                    'ui_id':ui_id,
                    'variant_id':variant_id,
                    'variant_name':variant_name,
                    'batch_id':batch_infos['id'] if batch_infos else None,
                    'batch_name':batch_infos['name'] if batch_infos else None,
                    'expiry_date':batch_infos['expiry_date'] if batch_infos else None,
                    'mfg_date':batch_infos['manufacturing_date'] if batch_infos else None,
                    'serial_numbers':serialno_infos if serialno_infos else None,
                    'type':update_type,
                    'stocks_before':stock_before,
                    'stocks_after':stock_after,
                    'stocks_adjusted':stocks
                }
            )

    stock_mov_adj_data={
        'shop_id':shop_id,
        'type':entity_name,
        'date':adj_date,
        'description':f'Stock adjusted via {entity_name}',
        'items':stock_mov_adj_items
    }

    ic(stock_mov_adj_data,stock_mov_adj_items)

    saga_id=generate_uuid()
    await SagaProducer.emit(
        saga_payload=CreateSagaStateSchema(
            id=saga_id,
            status=SagaStatusEnum.PENDING,
            type="STOCK_ADJUSTMENT",
            data={'stock_mov_adj':stock_mov_adj_data},
            steps={
                'STOCK_ADJUSTMENT_CREATION':SagaStepsValueEnum.PENDING
            },
            execution={
                'step':'STOCK_ADJUSTMENT_CREATION',
                'service':'STOCK_MOV_ADJ'
            }
        ),
        headers={
            "entity_name":"create_adjustment",
            "reply_key":'None',
            "reply_exchange":'None',
            "reply_entity_name":'None',
            "service_name":"STOCK_MOV_ADJ",
            "body":stock_mov_adj_data
        },
        routing_key="stockmovadj.service.routing.key",
        exchange_name="stockmovadj.service.exchange"
    )


    return True
    
    



    
    

