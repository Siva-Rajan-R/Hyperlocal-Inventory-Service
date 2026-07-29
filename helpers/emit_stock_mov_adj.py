from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime
from icecream import ic

from messaging.saga_producer import SagaProducer, SagaStateErrorTypDict, SagaStateExecutionTypDict, CreateSagaStateSchema
from infras.primary_db.repos.product_repo import ProductRepo
from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo
from infras.primary_db.main import AsyncSession
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from hyperlocal_platform.core.enums.saga_state_enum import SagaStatusEnum, SagaStepsValueEnum
from schemas.v1.product_schemas.request_schemas import GetBulkProductsById


async def emit_stock_mov_adj(session: AsyncSession, data: List[dict]) -> bool:
    """
    Emits stock movement adjustments via Saga orchestrators safely.
    Handles combinations of Variants, Batches, and Serial Numbers smoothly.
    """
    print("Inside emit stock mov adj")
    # prod_repo_obj = ProductRepo(session=session)
    prod_repo_obj = ProdInvReadDbRepo

    validated_data: Dict[str, List[dict]] = {}
    product_ids = []
    shop_id: Optional[str] = None
    
    # STEP-1: Group incoming payload datasets by Product ID
    for prod in data:
        product_id = prod['product_id']
        shop_id = prod['shop_id']
        
        if product_id not in validated_data:
            validated_data[product_id] = []
        
        validated_data[product_id].append(prod)
        if product_id not in product_ids:
            product_ids.append(product_id)
            
    # STEP-2: Fetch matching inventory profiles from Primary DB
    # product_res = await prod_repo_obj.get_bulk_products_by_id(
    #     data=GetBulkProductsById(shop_id=shop_id, id=product_ids)
    # )
    product_res = await prod_repo_obj.get_bulk_by_id(
        data=GetBulkProductsById(shop_id=shop_id, id=product_ids)
    )
    # ic(product_res)
    
    stock_mov_adj_items = []
    adj_date = datetime.now()
    entity_name = "STOCK_ADJUSTMENT"

    # STEP-3: Correlate payload adjustments against state profiles
    for prod_db in product_res:
        product_id = prod_db['id']
        product_name = prod_db.get('name', '')
        ui_id = prod_db.get('ui_id', '')
        category_infos = prod_db.get('category_infos') or {}
        unit_infos = prod_db.get('unit_infos') or {}

        category_id = category_infos.get('id') if isinstance(category_infos, dict) else None
        category_name = category_infos.get('name') if isinstance(category_infos, dict) else None
        unit_id = unit_infos.get('id') if isinstance(unit_infos, dict) else None
        unit_name = unit_infos.get('name') if isinstance(unit_infos, dict) else None
        
        type_infos = prod_db.get('type_infos') or {}
        has_variant = type_infos.get('has_variant') if type_infos and 'has_variant' in type_infos else prod_db.get('has_variant', False)
        has_batch = type_infos.get('has_batch') if type_infos and 'has_batch' in type_infos else prod_db.get('has_batch', False)
        has_serialno = type_infos.get('has_serialno') if type_infos and 'has_serialno' in type_infos else prod_db.get('has_serialno', False)

        strut_prod_res = validated_data.get(product_id)
        if not strut_prod_res:
            continue

        for val in strut_prod_res:
            b_info_val = val.get('batch_infos')
            batch_id = val.get('batch_id') or (b_info_val.get('id') if isinstance(b_info_val, dict) else None)
            variant_id = val.get('variant_id')
            update_type = val.get('type')
            entity_name = val.get('entity_name', entity_name)
            stocks_adjusted = float(val.get('stocks', 0))

            variant_name = ""
            batch_infos = {}
            serialno_infos = []
            stock_infos = {}

            # --- Pathway A: Variant Target Strategy ---
            if has_variant and variant_id:
                variants_raw = prod_db.get('variants')
                variant_data = None
                if isinstance(variants_raw, dict):
                    variant_data = variants_raw.get(variant_id)
                elif isinstance(variants_raw, list):
                    for v in variants_raw:
                        if v.get('id') == variant_id:
                            variant_data = v
                            break
                
                if variant_data:
                    variant_name = variant_data.get('name', '')
                    
                    if has_batch and batch_id:
                        batches_list = variant_data.get('batch_infos', []) or []
                        for batch in batches_list:
                            if batch.get('id') == batch_id or batch.get('name') == batch_id:
                                batch_infos = batch
                                break
                        stock_infos = batch_infos.get('stock_infos', {}) if batch_infos else {}
                        serialno_infos = batch_infos.get('serialno_infos', []) if (batch_infos and has_serialno) else []
                    else:
                        stock_infos = variant_data.get('stock_infos', {}) or {}
                        serialno_infos = variant_data.get('serialno_infos', []) if has_serialno else []
                else:
                    stock_infos = prod_db.get('stock_infos', {}) or {}

            # --- Pathway B: Standard Product Strategy ---
            else:
                if has_batch and batch_id:
                    batches_list = prod_db.get('batch_infos', []) or []
                    for batch in batches_list:
                        if batch.get('id') == batch_id or batch.get('name') == batch_id:
                            batch_infos = batch
                            break
                    stock_infos = batch_infos.get('stock_infos', {}) if batch_infos else {}
                    serialno_infos = batch_infos.get('serialno_infos', []) if (batch_infos and has_serialno) else []
                else:
                    stock_infos = prod_db.get('stock_infos', {}) or {}
                    serialno_infos = prod_db.get('serialno_infos', []) if has_serialno else []

            # Safely get current physical stocks metrics
            current_physical = float(stock_infos.get('physical_stocks', 0))

            # stock_before = physical stock BEFORE this adjustment
            # stock_after  = physical stock AFTER this adjustment
            if update_type == "INCREMENT":
                stock_before = current_physical - stocks_adjusted
                stock_after = current_physical
            else:
                stock_before = current_physical + stocks_adjusted
                stock_after = current_physical

            raw_serials = val.get('serial_numbers') or val.get('serialno_infos') or []
            extracted_serials = []
            for sn in raw_serials:
                if isinstance(sn, dict):
                    sn_name = sn.get('name') or sn.get('serial_no') or sn.get('serialno') or ''
                    if sn_name:
                        extracted_serials.append(sn_name)
                elif isinstance(sn, str):
                    extracted_serials.append(sn)

            stock_mov_adj_items.append({
                'product_id': product_id,
                'name': product_name,
                'ui_id': ui_id,
                'category_id': category_id,
                'category_name': category_name,
                'unit_id': unit_id,
                'unit_name': unit_name,
                'variant_id': variant_id,
                'variant_name': variant_name,
                'batch_id': batch_infos.get('id') if batch_infos else batch_id,
                'batch_name': batch_infos.get('name') if batch_infos else None,
                'exp_date': batch_infos.get('expiry_date') if batch_infos else None,
                'mfg_date': batch_infos.get('manufacturing_date') if batch_infos else None,
                'serial_numbers': extracted_serials if extracted_serials else None,
                'type': update_type,
                'stocks_before': stock_before,
                'stocks_after': stock_after,
                'stocks': stocks_adjusted
            })

    # STEP-4: Package Transaction context for Saga Engine Orchestration
    stock_mov_adj_data = {
        'shop_id': shop_id,
        'type': entity_name,
        'date': adj_date.isoformat(),
        'description': f'Stock adjusted via {entity_name}',
        'items': stock_mov_adj_items
    }

    ic(stock_mov_adj_data)

    saga_id = generate_uuid()
    await SagaProducer.emit(
        saga_payload=CreateSagaStateSchema(
            id=saga_id,
            status=SagaStatusEnum.PENDING,
            type="STOCK_ADJUSTMENT",
            data={'stock_mov_adj': stock_mov_adj_data},
            steps={
                'STOCK_ADJUSTMENT_CREATION': SagaStepsValueEnum.PENDING
            },
            execution={
                'step': 'STOCK_ADJUSTMENT_CREATION',
                'service': 'STOCK_MOV_ADJ'
            }
        ),
        headers={
            "entity_name": "create_adjustment",
            "reply_key": 'None',
            "reply_exchange": 'None',
            "reply_entity_name": 'None',
            "service_name": "STOCK_MOV_ADJ",
            "body": stock_mov_adj_data
        },
        routing_key="stockmovadj.service.routing.key",
        exchange_name="stockmovadj.service.exchange"
    )

    return True