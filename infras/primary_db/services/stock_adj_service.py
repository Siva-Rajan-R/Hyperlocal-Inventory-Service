from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.inventory_model import StockAdjustments,StockAdjustmentInventoryProducts
from schemas.v1.db_schemas.stock_adj_schema import CreateStockAdjDbSchema
from schemas.v1.request_schemas.stock_adj_schema import CreateStockAdjSchema,GetStockAdjByShopIdSchema,GetAllStockAdjSchema,GetStockAdjByIdSchema,GetStockAdjByInventoryIdSchema,CreateStockAdjOnlySchema,StockAdjInventoryProductOnlySchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List,Dict
from icecream import ic
from datetime import date,datetime
from ..repos.stock_adj_repo import StockAdjRepo
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from ..services.inventory_service import InventoryService
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum, StockAdjustmentMovementType
from infras.primary_db.repos.inventory_repo import InventoryRepo,BulkCheckInventorySchema
import httpx

ACTIVITY_LOG_URL = "http://127.0.0.1:8001/activity-logs"

async def _send_activity_log(shop_id: str, action: str, entity_id: str, description: str, changes: list = None):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(ACTIVITY_LOG_URL, json={
                "shop_id": shop_id,
                "user_name": "siva",
                "service": "Inventory",
                "action": action,
                "entity_type": "StockAdjustment",
                "entity_id": entity_id,
                "description": description,
                "changes": changes or []
            })
    except Exception as e:
        ic(f"Failed to log activity: {e}")


class StockAdjService(BaseServiceModel):
    def __init__(self, session:AsyncSession):
        self.stock_adj_repo_obj=StockAdjRepo(session=session)
        super().__init__(session)

    
    async def make_stock_adjustment(self, data:CreateStockAdjOnlySchema):
        stockadj_id=generate_uuid()
        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id
        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        inv_config = shop_config.get("product", {})
        prefix = inv_config.get("prefix", "PROD")
        start_from = inv_config.get("start_from", 1)
        
        adj_config = shop_config.get("stock_adjustment", {})
        adj_prefix = adj_config.get("prefix", "ADJ")
        adj_start_from = adj_config.get("start_from", 1)

        variant_toincr={}
        batch_toincr={}
        inventory_toincr={}
        serailno_incr={}

        variant_todecr={}
        batch_todecr={}
        inventory_todecr={}
        serailno_decr={}

        stock_adj_inv_prod_toadd=[]

        inv_repo_obj=InventoryRepo(session=self.session)
        inventory_tocheck,variant_tocheck,batch_tocheck,serialno_tocheck=[],[],[],[]
        for product in data.products:
            if product.inventory_id not in inventory_tocheck: inventory_tocheck.append(product.inventory_id)
            if product.variant_id and product.variant_id not in variant_tocheck: variant_tocheck.append(product.variant_id)
            if product.batch_id and product.batch_id not in batch_tocheck: batch_tocheck.append(product.batch_id)
            if product.serialno_id and product.serialno_id not in serialno_tocheck: serialno_tocheck.append(product.serialno_id)
            
        inv_checked_results=await inv_repo_obj.bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inventory_tocheck))
        variant_checked_results=await inv_repo_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=variant_tocheck)
        batch_checked_results=await inv_repo_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=batch_tocheck)
        
        structured_inv_result_data = {r['id']: r for r in inv_checked_results}
        structured_variant_result_data = {r['id']: r for r in variant_checked_results}
        structured_batch_result_data = {r['id']: r for r in batch_checked_results}

        readdb_products_toadd=[]
        total_items = 0
        total_quantity = 0.0
        total_stock_in = 0.0
        total_stock_out = 0.0

        for product in data.products:
            stocks:int=product.stocks
            variant_id:str=product.variant_id
            batch_id:str=product.batch_id
            serialno_id:str=product.serialno_id
            adjustment_type=product.type
            stocks_before=product.stocks_before

            is_increment = (adjustment_type == StockAdjustmentTypesEnum.INCREMENT.value or adjustment_type == StockAdjustmentTypesEnum.INCREMENT)
            is_decrement = (adjustment_type == StockAdjustmentTypesEnum.DECREMENT.value or adjustment_type == StockAdjustmentTypesEnum.DECREMENT)

            if is_increment:
                if variant_id:
                    variant_toincr[variant_id]=stocks
                if batch_id:
                    batch_toincr[batch_id]=stocks
                if serialno_id:
                    serailno_incr[serialno_id]=stocks
                inventory_toincr[product.inventory_id] = inventory_toincr.get(product.inventory_id, 0) + stocks

            elif is_decrement:
                if variant_id:
                    variant_todecr[variant_id]=stocks
                if batch_id:
                    batch_todecr[batch_id]=stocks
                if serialno_id:
                    serailno_decr[serialno_id]=stocks
                inventory_todecr[product.inventory_id] = inventory_todecr.get(product.inventory_id, 0) + stocks

            
            stock_adj_inv_prod_toadd.append(
                StockAdjustmentInventoryProducts(
                    inventory_id=product.inventory_id,
                    stockadjustment_id=stockadj_id,
                    variant_id=variant_id,
                    batch_id=batch_id,
                    stocks=stocks,
                    type=adjustment_type,
                    stocks_before=stocks_before
                )
            )

            from infras.read_db.models.stock_movement_model import StockMovementProduct, VariantInfo, BatchInfo, SerialInfo
            
            stocks_adjusted = stocks if is_increment else -stocks
            
            readdb_products_toadd.append(
                StockMovementProduct(
                    inventory_id=product.inventory_id,
                    ui_id=structured_inv_result_data[product.inventory_id]['ui_id'],
                    name=structured_inv_result_data.get(product.inventory_id, {}).get('name', str(product.inventory_id)),
                    stocks_before=stocks_before,
                    stocks_adjusted=stocks_adjusted,
                    stocks_after=stocks_before + stocks_adjusted,
                    type=adjustment_type,
                    variant=VariantInfo(
                        variant_id=variant_id,
                        variant_name=structured_variant_result_data.get(variant_id, {}).get('datas', {}).get('barcode', str(variant_id))
                    ) if variant_id else None,
                    batch=BatchInfo(
                        batch_id=batch_id,
                        batch_name=structured_batch_result_data.get(batch_id, {}).get('name') or str(batch_id),
                        mfg_date=structured_batch_result_data.get(batch_id, {}).get('manufacturing_date'),
                        exp_date=structured_batch_result_data.get(batch_id, {}).get('expiry_date')
                    ) if batch_id else None,
                    serial_info=SerialInfo(
                        serialno_id=serialno_id,
                        serial_numbers=product.serial_numbers or []
                    ) if serialno_id else None,
                    storage_location=structured_inv_result_data.get(product.inventory_id, {}).get('datas', {}).get('storage_location', '')
                )
            )
            total_items += 1
            total_quantity += stocks
            if is_increment:
                total_stock_in += stocks
            else:
                total_stock_out += stocks

        
        NEXT=False
        stockadj_repo_obj=StockAdjRepo(session=self.session)
        
        raw_sequence = await stockadj_repo_obj.get_next_sequence(data.shop_id, adj_start_from)
        ui_id_str = format_ui_id(adj_prefix, adj_start_from, raw_sequence)
        
        stock_adjtoadd=CreateStockAdjDbSchema(
            id=stockadj_id,
            ui_id=ui_id_str,
            shop_id=data.shop_id,
            adjusted_date=date.today(),
            movement_type=data.movement_type,
            description=data.description,
            datas=data.datas
        )

        stockadj_res=await stockadj_repo_obj.create(data=stock_adjtoadd)
        ic(stockadj_res)
        NEXT=stockadj_res
        ic(NEXT)
        if NEXT:
            stockadj_inv_prod_res=await stockadj_repo_obj.create_bulk_stockadj_inv_prod(datas=stock_adj_inv_prod_toadd)
            NEXT=stockadj_inv_prod_res

            from infras.read_db.repos.stock_movement_repo import StockMovementReadDbRepo, StockMovementStatsReadDbRepo
            from infras.read_db.models.stock_movement_model import StockMovementReadModel
            
            await StockMovementReadDbRepo.create_stock_movement(
                StockMovementReadModel(
                    stock_movement_id=stockadj_id,
                    ui_id=ui_id_str,
                    shop_id=data.shop_id,
                    movement_type=data.movement_type or StockAdjustmentMovementType.STOCK_ADJUSTMENT,
                    adjusted_date=datetime.combine(date.today(), datetime.min.time()),
                    description=data.description,
                    total_items=total_items,
                    total_quantity=total_quantity,
                    products=readdb_products_toadd
                )
            )

            is_purchase = (data.movement_type == StockAdjustmentMovementType.DIRECT)
            is_sales = (data.movement_type == StockAdjustmentMovementType.SALES)

            await StockMovementStatsReadDbRepo.update_stats(
                shop_id=data.shop_id,
                stock_in=total_stock_in,
                stock_out=total_stock_out,
                is_purchase=is_purchase,
                is_sales=is_sales
            )

            await _send_activity_log(
                shop_id=data.shop_id,
                action="CREATE",
                entity_id=stockadj_id,
                description=f"Created stock adjustment: {data.movement_type.value}",
                changes=[
                    {"field": "movement_type", "before": "", "after": str(data.movement_type.value)},
                    {"field": "total_items", "before": "", "after": str(total_items)}
                ]
            )

        ic(NEXT)
        return NEXT





    async def create_v2(self, data:CreateStockAdjSchema, can_update_stock: Optional[bool] = True):
        inv_repo_obj=InventoryRepo(session=self.session)
        stockadj_id=generate_uuid()
        structured_data:Dict[str,List[dict]]={}
        inventory_tocheck,variant_tocheck,batch_tocheck,serialno_tocheck=[],[],[],[]

        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id
        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        inv_config = shop_config.get("product", {})
        prefix = inv_config.get("prefix", "PROD")
        start_from = inv_config.get("start_from", 1)
        
        adj_config = shop_config.get("stock_adjustment", {})
        adj_prefix = adj_config.get("prefix", "ADJ")
        adj_start_from = adj_config.get("start_from", 1)


        from fastapi.exceptions import HTTPException
        from hyperlocal_platform.core.models.req_res_models import ErrorResponseTypDict

        # STEP-1 DUPLICATION IDENTIFICATION
        for product in data.products:
            if product.inventory_id not in structured_data:
                structured_data[product.inventory_id]=[]
            else:
                values=structured_data[product.inventory_id]
                for val in values:
                    variant_id,batch_id,serialno_id=val['variant_id'],val['batch_id'],val['serialno_id']
                    inc_variant_id,inc_batch_id,inc_serialno_id=product.variant_id,product.batch_id,product.serialno_id

                    if variant_id==inc_variant_id and batch_id==inc_batch_id:
                        ic("A same product or variant or batch appeared")
                        raise HTTPException(status_code=400, detail=ErrorResponseTypDict(msg="Duplicate product selection", status_code=400, description="The same product variant/batch was selected multiple times.", success=False))
        
            structured_data[product.inventory_id].append(product.model_dump())
            if product.inventory_id not in inventory_tocheck:
                inventory_tocheck.append(product.inventory_id)
            if product.variant_id and product.variant_id not in variant_tocheck:
                variant_tocheck.append(product.variant_id)
            if product.batch_id and product.batch_id not in batch_tocheck:
                batch_tocheck.append(product.batch_id)
            if product.serialno_id and product.serialno_id not in serialno_tocheck:
                serialno_tocheck.append(product.serialno_id)
        
        # DUPLICATION CHECKS END HERE
        

        ic(inventory_tocheck,variant_tocheck,batch_tocheck,serialno_tocheck)

        # STEP-2 DB CHECK
        inv_checked_results=await inv_repo_obj.bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inventory_tocheck))
        variant_checked_results=await inv_repo_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=variant_tocheck)
        batch_checked_results=await inv_repo_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=batch_tocheck)
        serialno_checked_results=await inv_repo_obj.bulk_serialno_check(shop_id=data.shop_id,serialnos_id=serialno_tocheck)

        if len(inventory_tocheck)!=len(inv_checked_results) or len(variant_tocheck)!=len(variant_checked_results) or len(batch_tocheck)!=len(batch_checked_results) or len(serialno_tocheck)!=len(serialno_checked_results):
            ic("Inventory,batch,variant,seriano some of these id was mistmatched")
            raise HTTPException(status_code=400, detail=ErrorResponseTypDict(msg="Validation Error", status_code=400, description="Some selected products, variants, or batches do not exist in the database.", success=False))
        

        structured_inv_result_data,structured_variant_result_data,structured_batch_result_data={},{},{}
        for result in inv_checked_results:
            structured_inv_result_data[result['id']]=result

        for result in variant_checked_results:
            structured_variant_result_data[result['id']]=result

        for result in batch_checked_results:
            structured_batch_result_data[result['id']]=result
        
        ic(structured_inv_result_data,structured_variant_result_data,structured_batch_result_data)

        # DB CHECK ENDS HERE


        # STEP-3 DB UPDATE
        variant_toincr={}
        batch_toincr={}
        inventory_toincr={}
        serailno_incr={}
        serailno_toadd=[]

        variant_todecr={}
        batch_todecr={}
        inventory_todecr={}
        serailno_decr={}

        stock_adj_inv_prod_toadd=[]
        readdb_products_toadd=[]
        total_items = 0
        total_quantity = 0.0
        total_stock_in = 0.0
        total_stock_out = 0.0

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
                stocks:int=val['stocks']
                variant_id:str=val.get('variant_id',None)
                batch_id:str=val.get('batch_id')
                serialno_id:str=val.get('serialno_id')
                serial_numbers:list[str]=val.get('serial_numbers',None)
                adjustment_type=val.get('type')


                inv_stocks_before=structured_inv_result_data[inventory_id]['stocks'] if inventory_id in structured_inv_result_data else 0.0
                batch_stocks_before=structured_batch_result_data[batch_id]['stocks'] if batch_id in structured_batch_result_data else 0.0
                variant_stocks_before=structured_variant_result_data[variant_id]['stocks'] if variant_id in structured_variant_result_data else 0.0
                

                if has_variant and not variant_id:
                    ic("Variant id does not exists")
                    raise HTTPException(status_code=400, detail=ErrorResponseTypDict(msg="Validation Error", status_code=400, description="Variant ID is required for variant products.", success=False))
                    
                if has_batch and not batch_id:
                    ic("batch id doesnot exists")
                    raise HTTPException(status_code=400, detail=ErrorResponseTypDict(msg="Validation Error", status_code=400, description="Batch ID is required for batch products.", success=False))

                if has_serialno:
                    if len(serial_numbers or []) != stocks:
                        ic("Serial numbers length mismatch")
                        raise HTTPException(status_code=400, detail=ErrorResponseTypDict(msg="Validation Error", status_code=400, description="Serial numbers length does not match the quantity.", success=False))
                    
                    is_decrement = (adjustment_type == StockAdjustmentTypesEnum.DECREMENT.value or adjustment_type == StockAdjustmentTypesEnum.DECREMENT)
                    if is_decrement and not serialno_id:
                        ic("Serialno ID is required for decrement")
                        raise HTTPException(status_code=400, detail=ErrorResponseTypDict(msg="Validation Error", status_code=400, description="Serial number ID is required for serial tracking deductions.", success=False))

                stocks_before=inv_stocks_before
                if has_variant:
                    stocks_before=variant_stocks_before
                if has_batch:
                    stocks_before=batch_stocks_before
                
                if has_variant and has_batch:
                    stocks_before=batch_stocks_before


                is_increment = (adjustment_type == StockAdjustmentTypesEnum.INCREMENT.value or adjustment_type == StockAdjustmentTypesEnum.INCREMENT)
                is_decrement = (adjustment_type == StockAdjustmentTypesEnum.DECREMENT.value or adjustment_type == StockAdjustmentTypesEnum.DECREMENT)

                if is_increment:
                    if has_variant and variant_id:
                        variant_toincr[variant_id]=stocks
                    if has_batch and batch_id:
                        batch_toincr[batch_id]=stocks

                    if has_serialno and not serialno_id and serial_numbers:
                        if batch_id:
                            serialno_id = structured_batch_result_data.get(batch_id, {}).get("serialno_id")
                        elif variant_id:
                            serialno_id = structured_variant_result_data.get(variant_id, {}).get("serialno_id")
                        elif inventory_id:
                            serialno_id = structured_inv_result_data.get(inventory_id, {}).get("serialno_id")

                    if has_serialno and serialno_id and serial_numbers:
                        serailno_incr.setdefault(serialno_id, []).extend(serial_numbers)

                    if has_serialno and not serialno_id and serial_numbers:
                        serialno_id = generate_uuid()
                        serailno_toadd.append(
                            InventorySerialNumbers(
                                id=serialno_id,
                                shop_id=data.shop_id,
                                inventory_id=inventory_id,
                                variant_id=variant_id,
                                batch_id=batch_id,
                                serial_numbers=serial_numbers
                            )
                        )

                    inv_stocks += stocks

                elif is_decrement:
                    if has_variant and variant_id:
                        variant_todecr[variant_id]=stocks
                    if has_batch and batch_id:
                        batch_todecr[batch_id]=stocks
                    if has_serialno and serialno_id:
                        serailno_decr[serialno_id]=serial_numbers
                    inv_stocks -= stocks

                
                stock_adj_inv_prod_toadd.append(
                    StockAdjustmentInventoryProducts(
                        inventory_id=inventory_id,
                        stockadjustment_id=stockadj_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        stocks=stocks,
                        type=adjustment_type,
                        stocks_before=stocks_before
                    )
                )

                from infras.read_db.models.stock_movement_model import StockMovementProduct, VariantInfo, BatchInfo, SerialInfo
                
                stocks_adjusted = stocks if is_increment else -stocks
                
                readdb_products_toadd.append(
                    StockMovementProduct(
                        inventory_id=inventory_id,
                        ui_id=structured_inv_result_data[inventory_id]['ui_id'],
                        name=structured_inv_result_data[inventory_id]['name'],
                        stocks_before=stocks_before,
                        stocks_adjusted=stocks_adjusted,
                        stocks_after=stocks_before + stocks_adjusted,
                        type=adjustment_type,
                        variant=VariantInfo(
                            variant_id=variant_id,
                            variant_name=structured_variant_result_data.get(variant_id, {}).get('datas', {}).get('barcode', str(variant_id))
                        ) if variant_id else None,
                        batch=BatchInfo(
                            batch_id=batch_id,
                            batch_name=structured_batch_result_data.get(batch_id, {}).get('name') or str(batch_id),
                            mfg_date=structured_batch_result_data.get(batch_id, {}).get('manufacturing_date'),
                            exp_date=structured_batch_result_data.get(batch_id, {}).get('expiry_date')
                        ) if batch_id else None,
                        serial_info=SerialInfo(
                            serialno_id=serialno_id,
                            serial_numbers=serial_numbers or []
                        ) if serialno_id else None,
                        storage_location=structured_inv_result_data[inventory_id].get('datas', {}).get('storage_location', '')
                    )
                )
                total_items += 1
                total_quantity += stocks
                if is_increment:
                    total_stock_in += stocks
                else:
                    total_stock_out += stocks
            
            if inv_stocks > 0:
                inventory_toincr[inventory_id] = inv_stocks
            elif inv_stocks < 0:
                inventory_todecr[inventory_id] = abs(inv_stocks)


        NEXT=False
        stockadj_repo_obj=StockAdjRepo(session=self.session)
        
        raw_sequence = await stockadj_repo_obj.get_next_sequence(data.shop_id, adj_start_from)
        ui_id_str = format_ui_id(adj_prefix, adj_start_from, raw_sequence)

        stock_adjtoadd=CreateStockAdjDbSchema(
            id=stockadj_id,
            ui_id=ui_id_str,
            shop_id=data.shop_id,
            adjusted_date=data.adjusted_date,
            movement_type=data.movement_type,
            description=data.description,
            datas=data.datas
        )

        stockadj_res=await stockadj_repo_obj.create(data=stock_adjtoadd)
        ic(stockadj_res)
        NEXT=stockadj_res
        ic(NEXT)
        if NEXT:
            stockadj_inv_prod_res=await stockadj_repo_obj.create_bulk_stockadj_inv_prod(datas=stock_adj_inv_prod_toadd)
            NEXT=stockadj_inv_prod_res
        ic(NEXT)
        if NEXT and can_update_stock:
            inv_repo_obj=InventoryRepo(session=self.session)
            if inventory_toincr:
                await inv_repo_obj.bulk_qty_update(data=inventory_toincr,shop_id=data.shop_id)
            if variant_toincr:
                await inv_repo_obj.bulk_variant_qty_update(data=variant_toincr,shop_id=data.shop_id)
            if batch_toincr:
                await inv_repo_obj.bulk_batch_qty_update(data=batch_toincr,shop_id=data.shop_id)
            if serailno_incr:
                await inv_repo_obj.bulk_add_serialno(data=serailno_incr,shop_id=data.shop_id)
            if serailno_toadd:
                self.session.add_all(serailno_toadd)

            if inventory_todecr:
                await inv_repo_obj.bulk_qty_decr_update(data=inventory_todecr,shop_id=data.shop_id)
            if variant_todecr:
                await inv_repo_obj.bulk_variant_decr_qty_update(data=variant_todecr,shop_id=data.shop_id)
            if batch_todecr:
                await inv_repo_obj.bulk_batch_decr_qty_update(data=batch_todecr,shop_id=data.shop_id)
            if serailno_decr:
                ic("Need to implement")
                await inv_repo_obj.bulk_update_serialno(data=serailno_decr,shop_id=data.shop_id)

            # Sync stock adjustments and nested array fields to Read DB
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            from infras.read_db.models.inventory_model import InventoryReadModel
            from schemas.v1.request_schemas.inventory_schema import GetInventoryByIdSchema
            
            # Use the sets of affected inventories to do full document replacements
            affected_inv_ids = set(inventory_toincr.keys()).union(set(inventory_todecr.keys()))
            
            if affected_inv_ids:
                for inv_id in affected_inv_ids:
                    raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inv_id))
                    if raw_inventory:
                        raw_dict = dict(raw_inventory)
                        await InventoryReadDbRepo.replace_inventory(InventoryReadModel(**raw_dict))

        if NEXT:
            from infras.read_db.repos.stock_movement_repo import StockMovementReadDbRepo, StockMovementStatsReadDbRepo
            from infras.read_db.models.stock_movement_model import StockMovementReadModel
            from datetime import datetime

            await StockMovementReadDbRepo.create_stock_movement(
                StockMovementReadModel(
                    stock_movement_id=stockadj_id,
                    ui_id=stockadj_res,
                    shop_id=data.shop_id,
                    movement_type=data.movement_type or StockAdjustmentMovementType.STOCK_ADJUSTMENT,
                    adjusted_date=datetime.combine(data.adjusted_date, datetime.min.time()),
                    description=data.description,
                    total_items=total_items,
                    total_quantity=total_quantity,
                    products=readdb_products_toadd
                )
            )

            is_purchase = (data.movement_type == StockAdjustmentMovementType.DIRECT)
            is_sales = (data.movement_type == StockAdjustmentMovementType.SALES)

            await StockMovementStatsReadDbRepo.update_stats(
                shop_id=data.shop_id,
                stock_in=total_stock_in,
                stock_out=total_stock_out,
                is_purchase=is_purchase,
                is_sales=is_sales
            )

            await _send_activity_log(
                shop_id=data.shop_id,
                action="CREATE",
                entity_id=stockadj_id,
                description=f"Created stock adjustment v2: {data.movement_type.value}",
                changes=[
                    {"field": "movement_type", "before": "", "after": str(data.movement_type.value)},
                    {"field": "total_items", "before": "", "after": str(total_items)}
                ]
            )

        return NEXT

    async def create(self, data:CreateStockAdjSchema,can_update_stock:Optional[bool]=True):
        ic(data)
        stockadj_id=generate_uuid()
        
        verified_inv_product=[]
        verified_variant=[]
        verified_batch=[]
        verified_serialno=[]

        checking_formatted_data={}

        for product in data.products:
            if (not product.batch_id and not product.variant_id) and product.inventory_id in verified_inv_product:
                ic("Same product should not be added twice")
                return False
            
            if product.variant_id and product.variant_id in verified_variant:
                ic("Same product + variant should not be added twice")
                return False
            
            if product.batch_id and product.batch_id in verified_batch:
                ic("Same product + batch should not be added twice")
                return False

            if product.serialno_id and product.serialno_id in verified_serialno:
                ic("Same product + serial number should not be added twice")
                return False

            if product.batch_id:
                verified_batch.append(product.batch_id)

            if product.variant_id:
                verified_variant.append(product.variant_id)

            if product.serialno_id:
                verified_serialno.append(product.serialno_id)
            
            if product.inventory_id not in verified_inv_product:
                verified_inv_product.append(product.inventory_id)

            if product.inventory_id not in checking_formatted_data:
                checking_formatted_data[product.inventory_id] = []

            checking_formatted_data[product.inventory_id].append(
                product.model_dump(mode="json")
            )

        ic(verified_inv_product, verified_variant, verified_batch, verified_serialno, checking_formatted_data)

        inv_repo_obj = InventoryRepo(session=self.session)
        inv_checked_results = await inv_repo_obj.bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=verified_inv_product))
        variant_checked_results = await inv_repo_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=verified_variant)
        batch_checked_results = await inv_repo_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=verified_batch)
        serialno_checked_results = await inv_repo_obj.bulk_serialno_check(shop_id=data.shop_id,serialnos_id=verified_serialno)

        structured_inventory = {}
        for result in inv_checked_results:
            structured_inventory[result['id']] = result

        structured_variant = {}
        for variant in variant_checked_results:
            structured_variant[variant['id']] = variant
        
        structured_batch = {}
        for batch in batch_checked_results:
            structured_batch[batch['id']] = batch

        structured_serialno = {}
        for serial in serialno_checked_results:
            structured_serialno[serial['id']] = serial

        ic(structured_inventory, structured_variant, structured_batch, structured_serialno)

        ic(len(verified_inv_product) != len(inv_checked_results),
           len(verified_variant) != len(variant_checked_results) ,
           len(verified_batch) != len(batch_checked_results) ,
           len(verified_serialno) != len(serialno_checked_results))

        if len(verified_inv_product) != len(inv_checked_results) or \
           len(verified_variant) != len(variant_checked_results) or \
           len(verified_batch) != len(batch_checked_results) or \
           len(verified_serialno) != len(serialno_checked_results):
            ic("Some of the IDs are mismatching/not found in primary DB")
            return False

        variant_toincr={}
        batch_toincr={}
        inventory_toincr={}
        serailno_incr={}

        variant_todecr={}
        batch_todecr={}
        inventory_todecr={}
        serailno_decr={}

        stock_adj_inv_prod_toadd=[]

        ERROR_OCCURED=False
        for inv_res in inv_checked_results:
            inv_prod_id = inv_res['id']
            has_variant = inv_res['has_variant']
            has_batch = inv_res['has_batch']
            has_serialno = inv_res['has_serialno']
            
            # Net change for this base product
            net_inv_change = 0.0

            for requested_data in checking_formatted_data[inv_prod_id]:
                batch_id:str=requested_data.get('batch_id',None)
                variant_id:str=requested_data.get('variant_id',None)
                serial_id:str=requested_data.get("serialno_id",None)
                stocks:float=requested_data['stocks']
                adjustment_type=requested_data['type']
                serial_numbers = requested_data.get('serial_numbers', []) or []

                ic(batch_id, inv_prod_id, variant_id, serial_id, stocks, adjustment_type)

                if has_variant and not variant_id:
                    ic("There is no variant id")
                    ERROR_OCCURED=True
                    return None
                
                if has_batch and not batch_id:
                    ic("There is no batch id")
                    ERROR_OCCURED=True
                    return None

                if has_serialno and not serial_id:
                    ic("Serial number ID is missing for serialized inventory")
                    ERROR_OCCURED=True
                    return None

                if has_serialno and len(serial_numbers) != stocks:
                    ic("Invalid Serial numbers length vs stocks count")
                    ERROR_OCCURED=True
                    return None

                # Determine the correct stocks_before
                stocks_before = inv_res['stocks']
                if has_variant:
                    stocks_before = structured_variant[variant_id]['stocks'] if variant_id in structured_variant else 0.0
                if has_batch:
                    stocks_before = structured_batch[batch_id]['stocks'] if batch_id in structured_batch else 0.0

                # Track quantities for updates
                is_increment = (adjustment_type == StockAdjustmentTypesEnum.INCREMENT.value or adjustment_type == StockAdjustmentTypesEnum.INCREMENT)
                is_decrement = (adjustment_type == StockAdjustmentTypesEnum.DECREMENT.value or adjustment_type == StockAdjustmentTypesEnum.DECREMENT)

                if is_increment:
                    if has_variant and variant_id:
                        variant_toincr[variant_id] = variant_toincr.get(variant_id, 0) + stocks
                    if has_batch and batch_id:
                        batch_toincr[batch_id] = batch_toincr.get(batch_id, 0) + stocks
                    if has_serialno and serial_id and serial_numbers:
                        serailno_incr.setdefault(serial_id, []).extend(serial_numbers)
                    net_inv_change += stocks

                elif is_decrement:
                    if has_variant and variant_id:
                        variant_todecr[variant_id] = variant_todecr.get(variant_id, 0) + stocks
                    if has_batch and batch_id:
                        batch_todecr[batch_id] = batch_todecr.get(batch_id, 0) + stocks
                    if has_serialno and serial_id and serial_numbers:
                        serailno_decr.setdefault(serial_id, []).extend(serial_numbers)
                    net_inv_change -= stocks

                stock_adj_inv_prod_toadd.append(
                    StockAdjustmentInventoryProducts(
                        inventory_id=inv_prod_id,
                        stockadjustment_id=stockadj_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        stocks=stocks,
                        type=adjustment_type,
                        stocks_before=stocks_before
                    )
                )

            # Apply net inventory change
            if net_inv_change > 0:
                inventory_toincr[inv_prod_id] = net_inv_change
            elif net_inv_change < 0:
                inventory_todecr[inv_prod_id] = abs(net_inv_change)

        ic(inventory_toincr,variant_toincr,batch_toincr,serailno_incr,stock_adj_inv_prod_toadd)
        ic(inventory_todecr,variant_todecr,batch_todecr,serailno_decr)
        if ERROR_OCCURED:
            ic("Error occured")
            return False

        NEXT=False
        stockadj_repo_obj=StockAdjRepo(session=self.session)

        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id
        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        adj_config = shop_config.get("stock_adjustment", {})
        adj_prefix = adj_config.get("prefix", "ADJ")
        adj_start_from = adj_config.get("start_from", 1)

        raw_sequence = await stockadj_repo_obj.get_next_sequence(data.shop_id, adj_start_from)
        ui_id_str = format_ui_id(adj_prefix, adj_start_from, raw_sequence)

        stock_adjtoadd=CreateStockAdjDbSchema(
            id=stockadj_id,
            ui_id=ui_id_str,
            shop_id=data.shop_id,
            adjusted_date=data.adjusted_date,
            movement_type=data.movement_type,
            description=data.description,
            datas=data.datas
        )

        stockadj_res=await stockadj_repo_obj.create(data=stock_adjtoadd)
        ic(stockadj_res)
        NEXT=stockadj_res
        ic(NEXT)
        if NEXT:
            stockadj_inv_prod_res=await stockadj_repo_obj.create_bulk_stockadj_inv_prod(datas=stock_adj_inv_prod_toadd)
            NEXT=stockadj_inv_prod_res
        ic(NEXT)
        if NEXT and can_update_stock:
            inv_repo_obj=InventoryRepo(session=self.session)
            if inventory_toincr:
                await inv_repo_obj.bulk_qty_update(data=inventory_toincr,shop_id=data.shop_id)
            if variant_toincr:
                await inv_repo_obj.bulk_variant_qty_update(data=variant_toincr,shop_id=data.shop_id)
            if batch_toincr:
                await inv_repo_obj.bulk_batch_qty_update(data=batch_toincr,shop_id=data.shop_id)
            if serailno_incr:
                await inv_repo_obj.bulk_add_serialno(data=serailno_incr,shop_id=data.shop_id)

            if inventory_todecr:
                await inv_repo_obj.bulk_qty_decr_update(data=inventory_todecr,shop_id=data.shop_id)
            if variant_todecr:
                await inv_repo_obj.bulk_variant_decr_qty_update(data=variant_todecr,shop_id=data.shop_id)
            if batch_todecr:
                await inv_repo_obj.bulk_batch_decr_qty_update(data=batch_todecr,shop_id=data.shop_id)
            if serailno_decr:
                ic("Need to implement")
                await inv_repo_obj.bulk_update_serialno(data=serailno_decr,shop_id=data.shop_id)

            await _send_activity_log(
                shop_id=data.shop_id,
                action="CREATE",
                entity_id=stockadj_id,
                description=f"Created stock adjustment: {data.movement_type.value}",
                changes=[
                    {"field": "movement_type", "before": "", "after": str(data.movement_type.value)},
                    {"field": "total_items", "before": "", "after": str(len(data.products))}
                ]
            )

        return NEXT

            


            

        

        

    async def create_bulk(self,datas:List[CreateStockAdjSchema]):
        datas_toadd=[]
        for data in datas:
            StockAdjustments(id=generate_uuid(),**data.model_dump(mode='json'))

        res=await self.stock_adj_repo_obj.add_all(datas_toadd)
        return res
    
    async def update(self,data:CreateStockAdjSchema):
        data_toupdate=StockAdjUpdateDbSchema(**data.model_dump(mode='json'))
        res=await self.stock_adj_repo_obj.update(data=data_toupdate)

        return res
        
    
    async def delete(self,stock_adj_id:str,shop_id:str):
        res=await self.stock_adj_repo_obj.delete(stock_adj_id=stock_adj_id,shop_id=shop_id)
        if res:
            await _send_activity_log(
                shop_id=shop_id,
                action="DELETE",
                entity_id=stock_adj_id,
                description=f"Deleted stock adjustment",
                changes=[{"field": "stock_adj_id", "before": str(stock_adj_id), "after": "DELETED"}]
            )
        return res
    
    async def bulk_check(self,shop_id:str,stock_adj_ids:list):
        res=await self.stock_adj_repo_obj.bulk_check(shop_id=shop_id,stock_adj_ids=stock_adj_ids)
        return res
        
    async def get(self,data:GetAllStockAdjSchema):
        res=await self.stock_adj_repo_obj.get(data=data)
        return res
    
    async def getby_shop_id(self,data:GetStockAdjByShopIdSchema):
        res=await self.stock_adj_repo_obj.getby_shop_id(data=data)
        return res
    
    async def getby_id(self,data:GetStockAdjByIdSchema):
        res=await self.stock_adj_repo_obj.getby_id(data=data)
        return res
    
    async def getby_inventory_id(self,data:GetStockAdjByInventoryIdSchema):
        res=await self.stock_adj_repo_obj.getby_inventory_id(data=data)
        return res

    async def search(self, shop_id: str, query: str, limit: int = 5):
        return await self.stock_adj_repo_obj.search(shop_id=shop_id, query=query, limit=limit)

        


        