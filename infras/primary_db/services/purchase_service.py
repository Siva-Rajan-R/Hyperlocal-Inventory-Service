from models.service_models.base_service_model import BaseServiceModel
from ..repos.purchase_repo import PurchaseRepo
from ..services.stock_adj_service import StockAdjService
from core.data_formats.enums.stock_adj_enums import StockAdjustmentMovementType,StockAdjustmentTypesEnum
from typing import Optional,List
from ..models.purchase_model import Purchase,PurchaseInventoryProducts
from schemas.v1.request_schemas.stock_adj_schema import CreateStockAdjSchema,StockAdjInventoryProductSchema,CreateStockAdjOnlySchema,StockAdjInventoryProductOnlySchema
from schemas.v1.db_schemas.stock_adj_schema import CreateStockAdjDbSchema
from ..models.inventory_model import InventoryBatches,InventorySerialNumbers
from sqlalchemy.ext.asyncio import AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from ..repos.inventory_repo import InventoryRepo
from schemas.v1.db_schemas.purchase_schema import CreatePurchaseDbSchema,UpdatePurchaseDbSchema
from schemas.v1.request_schemas.inventory_schema import BulkCheckInventorySchema,InventoryBatchSchema,GetAllInventorySchema
from schemas.v1.db_schemas.inventory_schema import InventoryBatchDbSchema,InventorySerialNumberDbSchema
from schemas.v1.request_schemas.purchase_schema import CreatePurchaseSchema,BulkCheckPurchaseSchema,GetPurchaseByShopIdSchema,GetPurchaseByIdSchema,GetPurchaseByInventoryIdSchema,GetPurchaseBySupplierIdSchema
from core.errors.messaging_errors import BussinessError,FatalError,RetryableError
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums,PurchaseViewsEnums
from .inventory_service import InventoryService
from icecream import ic
from typing import Union,List,Dict
from datetime import date
from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
from infras.read_db.models.purchase_model import PurchaseReadModel,SupplierInfo,PurchaseProduct,VariantInfo,BatchInfo,SerialInfo
import httpx

ACTIVITY_LOG_URL = "http://127.0.0.1:8001/activity-logs"

async def _send_activity_log(shop_id: str, action: str, entity_id: str, description: str, changes: list = None):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(ACTIVITY_LOG_URL, json={
                "shop_id": shop_id,
                "user_name": "siva",
                "service": "Purchase",
                "action": action,
                "entity_type": "Purchase",
                "entity_id": entity_id,
                "description": description,
                "changes": changes or []
            })
    except Exception as e:
        ic(f"Failed to log activity: {e}")


class PurchaseService(BaseServiceModel):
    def __init__(self, session:AsyncSession):
        self.session=session
        self.purchase_repo_obj=PurchaseRepo(session=session)

    async def create(self,data:CreatePurchaseSchema):
        # variant should need to check
        purchase_id=generate_uuid()
        purchase_view=True

        if data.type.value == PurchaseTypeEnums.PO_CREATE.value:
            purchase_view=False
        if data.type.value==PurchaseTypeEnums.PO_UPDATE.value and not data.purchase_id:
            ic("Purchase id not found for the po update")
            return False
        if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
            purchase_id=data.purchase_id

        if not purchase_id:
            ic("Purchase id Not Found")
            return False
        
        ic(purchase_id)
        inventories_tocheck=[]
        formatted_req_inventories={}
        po_checked_results=[]

        for inventory in data.products:
            ic(inventories_tocheck)
            ic(formatted_req_inventories)
            exists_data=formatted_req_inventories.get(inventory.inventory_id,None)
            ic(exists_data)
            if exists_data:
                if exists_data['variant_id'] and inventory.variant_id:
                    if exists_data['variant_id'] == inventory.variant_id:
                        ic("Same Product+variant does not add twice")
                        return False
                    
                if exists_data['batch_id'] and inventory.batch_id:
                    if exists_data['batch_id'] == inventory.batch_id:
                        ic("Same Product+batch does not add twice")
                        return False
                
                if (not exists_data['variant_id'] and not exists_data['batch_id']) and exists_data['inventory_id']==inventory.inventory_id:
                    ic("Same Product does not add twice")
                    return False


                    
                    
            inventories_tocheck.append(inventory.inventory_id)
            formatted_req_inventories[inventory.inventory_id]=inventory.model_dump(mode="json")

        ic(inventories_tocheck,formatted_req_inventories)
        
        if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
            po_checked_results=await PurchaseRepo(session=self.session).bulk_check_purchase_inv_products(BulkCheckPurchaseSchema(purchase_id=purchase_id,inventory_id=inventories_tocheck))
            if not po_checked_results or len(po_checked_results)!=len(data.products):
                ic("Po Check Failed")
                return False
        

            

        checked_results=await InventoryRepo(session=self.session).bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inventories_tocheck))
        
        

        ic(checked_results)
        ic(data.products)
        ic(inventories_tocheck)

        ic(len(checked_results),len(data.products),len(inventories_tocheck))


        if len(checked_results)!=len(inventories_tocheck):
            ic("Invalid checked results")
            return False
        
        batch_toupdate={}
        batch_toadd=[]
        variant_toudate=[]
        product_toupdate=[]
        serialno_toupdate={}


        purchase_inv_product_toadd=[]
        purchace_inv_product_toupdate=[]
        serialno_toadd=[]


        
        ERROR_OCCURED=False
        for inv_res in checked_results:
            ic(inv_res)
            requested_data=formatted_req_inventories.get(inv_res['id'],None)
            ic(requested_data)
            if not requested_data:
                ic("Requested data not found")
                ERROR_OCCURED=True
                return False

            batch_id:str=requested_data.get('batch_id',None)
            batch:InventoryBatchSchema=requested_data['batch']
            inv_id:str=requested_data.get('inventory_id',None)
            variant_id:str=requested_data.get('variant_id',None)
            serial_id:list=requested_data.get("serialno_id",None)

            stocks:int=requested_data['stocks']
            received_stocks=stocks

            if data.type.value==PurchaseTypeEnums.PO_UPDATE.value and not requested_data['received_stocks']:
                ic("Received stock not found")
                ERROR_OCCURED=True
                return False
            ic(data.type.value==PurchaseTypeEnums.PO_UPDATE.value)
            if data.type.value==PurchaseTypeEnums.PO_UPDATE.value:
                received_stocks=requested_data['received_stocks']
            
            ic(received_stocks)
            

            if data.type!=PurchaseTypeEnums.PO_CREATE:
                if inv_res['has_variant'] and not variant_id:
                    ic("There is no variant id")
                    ERROR_OCCURED=True
                    return None
                
                if inv_res['has_batch'] and not batch_id and not batch:
                    ic("There is no batch id")
                    ERROR_OCCURED=True
                    return None
                
                new_batch_id=None
                if inv_res['has_batch'] and not batch_id and batch:
                    new_batch_id=generate_uuid()
                    batch_data_toadd=InventoryBatchDbSchema(
                        id=new_batch_id,
                        shop_id=data.shop_id,
                        inventory_id=inv_id,
                        variant_id=variant_id,
                        name=batch['name'],
                        expiry_date=batch['expiry_date'],
                        manufacturing_date=batch['manufacturing_date'],
                        stocks=stocks  
                    )
                    batch_toadd.append(
                        InventoryBatches(**batch_data_toadd.model_dump())
                    )

                

                if inv_res['has_variant'] and variant_id:
                    variant_toudate.append(
                        {
                            'b_id':variant_id,
                            'stocks':received_stocks,
                            'is_absolute':False,
                            'buy_price':requested_data['buy_price'],
                            'sell_price':requested_data['sell_price'],
                            'reorder_point':requested_data['reorder_point'],
                            'datas':requested_data.get('datas', None)
                        }
                    )
                
                if inv_res['has_batch'] and batch_id:
                    batch_toupdate[batch_id]=received_stocks

                if inv_res['has_serialno'] and len(requested_data.get('serial_numbers',[]) or [])!=received_stocks:
                    ic("Invalid serial numbers")
                    ERROR_OCCURED=True
                    return False

                if (inv_res['has_serialno'] and serial_id) and len(requested_data.get('serial_numbers',[]) or [])==received_stocks:
                    serialno_toupdate[serial_id]=requested_data['serial_numbers']

                if (inv_res['has_serialno'] and not serial_id) and len(requested_data.get('serial_numbers',[]) or [])==received_stocks:
                    serialno_data_toadd=InventorySerialNumberDbSchema(
                        id=generate_uuid(),
                        shop_id=data.shop_id,
                        inventory_id=inv_id,
                        batch_id=new_batch_id,
                        variant_id=variant_id,
                        serial_numbers=requested_data['serial_numbers']
                    )

                    serialno_toadd.append(InventorySerialNumbers(**serialno_data_toadd.model_dump()))
                
                product_toupdate.append(
                    {
                        'b_id':inv_id,
                        'stocks':received_stocks,
                        'is_absolute':False,
                        'buy_price':requested_data['buy_price'],
                        'sell_price':requested_data['sell_price'],
                        'reorder_point':requested_data['reorder_point'],
                        'is_active':True,
                        'datas': {'supplier': data.supplier_id} if getattr(data, 'supplier_id', None) else requested_data.get('datas', None)
                    }
                )



            purchase_inv_product_toadd.append(
                PurchaseInventoryProducts(
                    inventory_id=inv_id,
                    purchase_id=purchase_id,
                    variant_id=variant_id,
                    batch_id=batch_id,
                    stocks=stocks,
                    sell_price=requested_data['sell_price'],
                    buy_price=requested_data['buy_price'],
                    received_stocks=received_stocks,
                    stocks_before=inv_res['stocks']
                )
            )


            
        
        if ERROR_OCCURED:
            ic("Error Occured")
            return False
        
        ic(data)
        ic(batch_toupdate,variant_toudate,product_toupdate,purchase_inv_product_toadd,serialno_toupdate,serialno_toadd)

        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id

        pur_repo_obj=PurchaseRepo(session=self.session)

        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        pur_config = shop_config.get("purchase", {})
        prefix = pur_config.get("prefix", "PUR")
        start_from = pur_config.get("start_from", 1)

        raw_sequence = await pur_repo_obj.get_next_sequence(data.shop_id, start_from)
        ui_id_str = format_ui_id(prefix, start_from, raw_sequence)

        data_toadd=CreatePurchaseDbSchema(
            **data.model_dump(mode='json'),
            id=purchase_id,
            ui_id=ui_id_str,
            purchase_view=purchase_view
        )
        NEXT=False
        NEXT=False

       
        if data.type==PurchaseTypeEnums.PO_UPDATE:
            ic(po_checked_results)
            for pur_inv_id in po_checked_results:
                purchace_inv_product_toupdate.append(
                    {
                        'b_purchase_inv_id':pur_inv_id,
                        'b_received_stocks':received_stocks
                    }
            )
                
            ic(purchace_inv_product_toupdate)
                
            pur_item_res=await pur_repo_obj.update_purchase_inv_bulk(datas=purchace_inv_product_toupdate)
            ic(pur_item_res)

            

        if data.type!=PurchaseTypeEnums.PO_UPDATE:
            pur_res=await pur_repo_obj.create(data=data_toadd)
            NEXT=pur_res

            if NEXT:
                pur_inv_res=await pur_repo_obj.create_purchase_inv_bulk(data=purchase_inv_product_toadd)
                NEXT=pur_inv_res

        if data.type==PurchaseTypeEnums.PO_UPDATE:
            pur_update_res=await pur_repo_obj.update(data=UpdatePurchaseDbSchema(shop_id=data.shop_id,id=purchase_id,purchase_view=purchase_view))
            NEXT=pur_update_res

        if NEXT and data.type.value!=PurchaseTypeEnums.PO_CREATE.value:
            inv_repo_obj=InventoryRepo(session=self.session)
            await InventoryRepo(session=self.session).create_batch_bulk(datas=batch_toadd)
            await inv_repo_obj.create_serialno_bulk(datas=serialno_toadd)

            await inv_repo_obj.update_bulk(datas=product_toupdate)
            await inv_repo_obj.update_variant_bulk(datas=variant_toudate)
            await inv_repo_obj.bulk_batch_qty_update(data=batch_toupdate,shop_id=data.shop_id)
            await inv_repo_obj.bulk_add_serialno(data=serialno_toupdate,shop_id=data.shop_id)
            
            # Sync stock increments and fields to Read DB
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            from infras.read_db.models.inventory_model import InventoryReadModel
            from schemas.v1.request_schemas.inventory_schema import GetInventoryByIdSchema
            
            # Fetch perfectly updated state from Postgres and push to MongoDB
            for item in product_toupdate:
                inv_id = item['b_id']
                raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inv_id))
                if raw_inventory:
                    await InventoryReadDbRepo.replace_inventory(InventoryReadModel(**raw_inventory))

            NEXT=True

        if NEXT:
            product_count = len(data.products)
            await _send_activity_log(
                shop_id=data.shop_id,
                action="CREATE",
                entity_id=purchase_id,
                description=f"Created purchase entry ({data.type.value}) with {product_count} product(s)",
                changes=[
                    {"field": "type", "before": "", "after": str(data.type.value)},
                    {"field": "products", "before": "", "after": str(product_count)}
                ]
            )

        return NEXT


    async def update(self,data:dict,shop_id:str):
        ic(data.datas.id,data.datas.shop_id)
        purchase=await self.getby_id(purchase_id=data.datas.id,shop_id=data.datas.shop_id)
        ic(purchase)
        if not purchase:
            return False

        ic(len(purchase['datas']['products']),len(data.datas.products))
        if len(purchase['datas']['products'])!=len(data.datas.products):
            return False
        
        purchase_view=False
        if data.datas.type==PurchaseTypeEnums.PO_UPDATE.value.value:
            purchase_view=True
        
        inventory_update_res=await purchase_helper(session=self.session,data=data,shop_id=shop_id)
        ic(inventory_update_res)
        if not inventory_update_res:
            return False
        ic(purchase_view)
        data_toupdate=UpdatePurchaseDbSchema(
            datas=data.datas.model_dump(mode='json'),
            shop_id=data.datas.shop_id,
            id=data.datas.id,
            type=data.datas.type,
            purchase_view=purchase_view
        )

        res = await self.purchase_repo_obj.update(data=data_toupdate)
        return res
    

    async def delete(self,shop_id:str,id:str):
        res = await self.purchase_repo_obj.delete(id=id,shop_id=shop_id)
        if res:
            await _send_activity_log(
                shop_id=shop_id,
                action="DELETE",
                entity_id=id,
                description=f"Deleted purchase entry",
                changes=[{"field": "purchase_id", "before": str(id), "after": "DELETED"}]
            )
        return res

    async def get(self,data:GetPurchaseByShopIdSchema):
        return await self.purchase_repo_obj.get(data=data)
    
    async def get_by_inventory_id(self,data:GetPurchaseByInventoryIdSchema):
        res=await self.purchase_repo_obj.getby_inventory_id(data=data)
        return res
    
    async def getby_supplier_id(self,data:GetPurchaseBySupplierIdSchema):
        res=await self.purchase_repo_obj.getby_supplier_id(data=data)
        return res



    async def getby_id(self,data:GetPurchaseByIdSchema):
        return await self.purchase_repo_obj.getby_id(data=data)

    async def search(self, shop_id: str, query: str, limit: int = 5):
        return await self.purchase_repo_obj.search(shop_id=shop_id, query=query,limit=limit)
    

    async def create_direct_purchase(self,data:CreatePurchaseSchema):

        inv_repo_obj=InventoryRepo(session=self.session)
        purchase_id=generate_uuid()
        
        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id
        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        inv_config = shop_config.get("product", {})
        prefix = inv_config.get("prefix", "PROD")
        start_from = inv_config.get("start_from", 1)
        
        inventory_tocheck:list=[]
        variant_tocheck:list=[]
        batch_tocheck:list=[]
        serialno_tocheck:list=[]

        verified_inv_product=[]
        verified_variant=[]
        verified_batch=[]
        verified_serialno=[]

        checking_foramtted_data={}


        products=data.products

        for product in products:
            if (not product.batch_id and not product.variant_id) and product.inventory_id in verified_inv_product:
                ic("Same product should not be added twice")
                return False
            
            if product.variant_id and product.variant_id in verified_variant:
                ic("Same product + variant should not be added twice")
                return False
            
            if product.batch_id and product.batch in verified_batch:
                ic("Same product + batch should not be added twice")
                return False
            
            if product.batch_id:
                batch_tocheck.append(product.batch_id)
                verified_batch.append(product.batch_id)

            if product.variant_id:
                variant_tocheck.append(product.variant_id)
                verified_variant.append(product.variant_id)

            if product.serialno_id:
                serialno_tocheck.append(product.serialno_id)
                verified_serialno.append(product.serialno_id)
            
            if product.inventory_id not in verified_inv_product:
                inventory_tocheck.append(product.inventory_id)
                verified_inv_product.append(product.inventory_id)

            if product.inventory_id not in checking_foramtted_data:
                checking_foramtted_data[product.inventory_id] = []

            checking_foramtted_data[product.inventory_id].append(
                product.model_dump()
            )

        ic(inventory_tocheck,batch_tocheck,serialno_tocheck,variant_tocheck,checking_foramtted_data,verified_inv_product,verified_batch,verified_serialno,verified_variant)

        inv_checked_results=await inv_repo_obj.bulk_check(data=BulkCheckInventorySchema(shop_id=data.shop_id,id=inventory_tocheck))
        variant_checked_results=await inv_repo_obj.bulk_varient_check(shop_id=data.shop_id,variants_id=variant_tocheck)
        batch_checked_results=await inv_repo_obj.bulk_batch_check(shop_id=data.shop_id,batches_id=batch_tocheck)
        serialno_checked_results=await inv_repo_obj.bulk_serialno_check(shop_id=data.shop_id,serialnos_id=serialno_tocheck)

        structured_variant={}
        structured_batch={}
        for variant in variant_checked_results:
            structured_variant[variant['id']]=variant
        
        for batch in batch_checked_results:
            structured_batch[batch['id']]=batch

        ic(structured_batch,structured_variant)

        ic(inv_checked_results,variant_checked_results,batch_checked_results,serialno_tocheck)
        ic(len(inventory_tocheck)!=len(inv_checked_results) , len(batch_tocheck)!=len(batch_checked_results) , len(variant_tocheck)!=len(variant_checked_results), len(serialno_tocheck)!=len(serialno_checked_results))

        if len(inventory_tocheck)!=len(inv_checked_results) or len(batch_tocheck)!=len(batch_checked_results) or len(variant_tocheck)!=len(variant_checked_results) or len(serialno_tocheck)!=len(serialno_checked_results):
            ic("some of the id's are mistmatching")
            return False
        
        inv_prod_toupdate:List[dict]=[]
        variant_toupdate:List[dict]=[]
        batch_toupdate:dict={}
        serialno_toupdate:dict[str,List[str]]={}

        batch_toadd:List[InventoryBatches]=[]
        serialno_toadd:List[InventorySerialNumbers]=[]

        stock_adj_products:List[StockAdjInventoryProductSchema]=[]
        purchase_inv_product_toadd=[]

        for result in inv_checked_results:
            inv_prod_id=result['id']
            has_variant=result['has_variant']
            has_batch=result['has_batch']
            has_serialno=result['has_serialno']
            inv_stocks=0

            ic(inv_prod_id,has_batch,has_variant,has_serialno)

            for val in checking_foramtted_data[inv_prod_id]:
                ic(val)
                stocks:int=val['stocks']
                variant_id:str=val.get('variant_id',None)
                batch_id:str=val.get('batch_id')
                serialno_id:str=val.get('serialno_id')
                buy_price:float=val['buy_price']
                sell_price:float=val['sell_price']
                reorder_point:int=val['reorder_point']
                batch:InventoryBatchSchema=InventoryBatchSchema(**val['batch']) if val.get('batch',None) else None
                serial_numbers:list[str]=val.get('serial_numbers',None)
                inv_stocks+=stocks

                ic(val,stocks,variant_id,batch_id,serialno_id,buy_price,sell_price,batch,serial_numbers)

                if has_variant and not variant_id:
                    ic("Variant id not found")
                    return False
                
                if has_batch and (not batch_id and not batch):
                    ic("Batch id not found")
                    return False
                
                if has_serialno and (not serialno_id and not serial_numbers):
                    ic('Serial no id not found')
                    return False
                
                if serial_numbers and len(serial_numbers)!=stocks:
                    ic("Serial number does not matches the stocks")
                    return False


                if variant_id:
                    variant_toupdate.append(
                        {
                            'b_id':variant_id,
                            'is_absolute':False,
                            'stocks':stocks,
                            'buy_price':buy_price,
                            'sell_price':sell_price,
                            'reorder_point':reorder_point,
                            'datas':val.get('datas', None)
                        }
                    )

                if batch_id:
                    batch_toupdate[batch_id]=stocks

                if not batch_id and batch:
                    batch_id=generate_uuid()
                    batch_toadd.append(
                        InventoryBatches(
                            id=batch_id,
                            shop_id=data.shop_id,
                            inventory_id=inv_prod_id,
                            variant_id=variant_id,
                            stocks=stocks,
                            expiry_date=batch.expiry_date,
                            manufacturing_date=batch.manufacturing_date,
                            name=batch.name,
                            datas={}
                        )
                    )

                if not serialno_id and serial_numbers:
                    if batch_id:
                        serialno_id = structured_batch_result_data.get(batch_id, {}).get("serialno_id")
                    elif variant_id:
                        serialno_id = structured_variant_result_data.get(variant_id, {}).get("serialno_id")
                    elif inv_prod_id:
                        serialno_id = structured_inv_result_data.get(inv_prod_id, {}).get("serialno_id")

                if serialno_id and serial_numbers:
                    serialno_toupdate.setdefault(serialno_id, []).extend(serial_numbers)

                if not serialno_id and serial_numbers:
                    serialno_id=generate_uuid()
                    serialno_toadd.append(
                        InventorySerialNumbers(
                            id=serialno_id,
                            shop_id=data.shop_id,
                            inventory_id=inv_prod_id,
                            variant_id=variant_id,
                            batch_id=batch_id,
                            serial_numbers=serial_numbers
                        )
                    )

                stocks_before=result['stocks']
                if has_variant:
                    variant_exisist=structured_variant.get(variant_id)
                    stocks_before=variant_exisist['stocks'] if variant_exisist else 0.0
                
                if has_batch:
                    batch_exists=structured_batch.get(batch_id)
                    stocks_before=batch_exists['stocks'] if batch_exists else 0.0


                ic(stocks_before)
                
                purchase_inv_product_toadd.append(
                    PurchaseInventoryProducts(
                        inventory_id=inv_prod_id,
                        purchase_id=purchase_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        stocks=stocks,
                        sell_price=sell_price,
                        buy_price=buy_price,
                        received_stocks=stocks,
                        stocks_before=stocks_before
                    )
                )

                stock_adj_products.append(
                    StockAdjInventoryProductSchema(
                        inventory_id=inv_prod_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        serialno_id=serialno_id,
                        serial_numbers=serial_numbers,
                        stocks=stocks,
                        type=StockAdjustmentTypesEnum.INCREMENT
                    )
                )
                
            inv_prod_toupdate.append(
                {
                    'b_id':inv_prod_id,
                    'is_absolute':False,
                    'stocks':inv_stocks,
                    'buy_price':buy_price,
                    'sell_price':sell_price,
                    'reorder_point':reorder_point,
                    'is_active':True,
                    'datas': {'supplier': data.supplier_id} if getattr(data, 'supplier_id', None) else None
                }
            )

            ic(inv_stocks)


        ic(inv_prod_toupdate,variant_toupdate,batch_toupdate,serialno_toupdate,batch_toadd,serialno_toadd)

        data_toadd=CreatePurchaseDbSchema(
            **data.model_dump(mode='json'),
            id=purchase_id,
            purchase_view=True
        )
        pur_repo_obj=PurchaseRepo(session=self.session)

        res_ui_id=await pur_repo_obj.create(data=data_toadd)
        ic("hello p")
        ic(res_ui_id)
        if res_ui_id:
            pur_inv_res=await pur_repo_obj.create_purchase_inv_bulk(data=purchase_inv_product_toadd)
            ic(pur_inv_res)
            ic(stock_adj_products)
            await inv_repo_obj.create_batch_bulk(datas=batch_toadd)
            await inv_repo_obj.create_serialno_bulk(datas=serialno_toadd) 
            stock_res=await StockAdjService(session=self.session).create(
                can_update_stock=False,
                data=CreateStockAdjSchema(
                    shop_id=data.shop_id,
                    adjusted_date=date.today(),
                    movement_type=StockAdjustmentMovementType.DIRECT,
                    description="Stock Increased via purchase",
                    products=stock_adj_products
                )
            )
            ic(stock_res)
            await inv_repo_obj.update_bulk(datas=inv_prod_toupdate)
            await inv_repo_obj.update_variant_bulk(datas=variant_toupdate)
            await inv_repo_obj.bulk_batch_qty_update(data=batch_toupdate,shop_id=data.shop_id)
            await inv_repo_obj.bulk_add_serialno(data=serialno_toupdate,shop_id=data.shop_id)
            
            # Sync stock increments and fields to Read DB
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            from infras.read_db.models.inventory_model import InventoryReadModel
            from schemas.v1.request_schemas.inventory_schema import GetInventoryByIdSchema
            
            for item in inv_prod_toupdate:
                inv_id = item['b_id']
                raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inv_id))
                if raw_inventory:
                    await InventoryReadDbRepo.replace_inventory(InventoryReadModel(**raw_inventory))

            # await inv_repo_obj.create_batch_bulk(datas=batch_toadd)
            # await inv_repo_obj.create_serialno_bulk(datas=serialno_toadd)


            


            ic("all of them updated successfully")

            product_count = len(data.products)
            await _send_activity_log(
                shop_id=data.shop_id,
                action="CREATE",
                entity_id=purchase_id,
                description=f"Created direct purchase entry with {product_count} product(s)",
                changes=[
                    {"field": "type", "before": "", "after": "DIRECT_PURCHASE"},
                    {"field": "products", "before": "", "after": str(product_count)}
                ]
            )

            return True
        return False
    


    async def edit_direct_purchase(self, data: CreatePurchaseSchema, user_id: str):
        from fastapi import HTTPException
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
        from infras.read_db.models.purchase_model import PurchaseReadModel, SupplierInfo, PurchaseProduct
        from schemas.v1.request_schemas.inventory_schema import GetInventoryByIdSchema
        from datetime import datetime
        from sqlalchemy import delete
        
        # 1. Invoice Uniqueness Check
        invoice_no = data.datas.get("invoice_no") if data.datas else None
        if invoice_no:
            is_unique = await PurchaseReadDbRepo.check_invoice_uniqueness(
                shop_id=data.shop_id,
                supplier_id=data.supplier_id,
                invoice_no=invoice_no,
                exclude_purchase_id=data.purchase_id
            )
            if not is_unique:
                raise HTTPException(status_code=400, detail={"success": False, "msg": "Invoice number must be unique for this supplier."})

        # 2. Fetch existing purchase from Read DB
        old_purchase = await PurchaseReadDbRepo.get_purchase_by_id(GetPurchaseByIdSchema(id=data.purchase_id, shop_id=data.shop_id))
        if not old_purchase:
            raise HTTPException(status_code=404, detail={"success": False, "msg": "Purchase not found."})

        old_products_map = {p.inventory_id: {"stocks": p.stocks_added, "serials": p.serial_info.serial_numbers if p.serial_info else []} for p in old_purchase.products}

        inv_repo_obj = InventoryRepo(session=self.session)
        pur_repo_obj = PurchaseRepo(session=self.session)
        
        variant_tocheck = [p.variant_id for p in data.products if p.variant_id]
        batch_tocheck = [p.batch_id for p in data.products if p.batch_id]
        
        structured_variant = {}
        structured_batch = {}
        if variant_tocheck:
            variant_checked_results = await inv_repo_obj.bulk_varient_check(shop_id=data.shop_id, variants_id=variant_tocheck)
            structured_variant = {res['id']: res for res in variant_checked_results}
            
        if batch_tocheck:
            batch_checked_results = await inv_repo_obj.bulk_batch_check(shop_id=data.shop_id, batches_id=batch_tocheck)
            structured_batch = {res['id']: res for res in batch_checked_results}

        stock_adj_products = []
        inv_prod_toupdate = []
        readdb_products_toadd = []
        purchase_inv_product_toadd = []
        
        # Delete old lines
        await self.session.execute(delete(PurchaseInventoryProducts).where(PurchaseInventoryProducts.purchase_id == data.purchase_id))

        # 3. Calculate Deltas and Update Stock
        for p in data.products:
            old_product_info = old_products_map.get(p.inventory_id, {"stocks": 0.0, "serials": []})
            old_stock = old_product_info["stocks"]
            old_serials = old_product_info["serials"]
            delta_stock = p.stocks - old_stock
            
            if p.inventory_id in old_products_map:
                del old_products_map[p.inventory_id]
                
            raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=p.inventory_id))
            if not raw_inventory:
                continue
                
            raw_dict = dict(raw_inventory)
            current_inv_stock = raw_dict.get("stocks", 0.0)
            new_inv_stock = current_inv_stock + delta_stock
            
            if new_inv_stock < 0:
                raise HTTPException(status_code=400, detail={"success": False, "msg": f"Cannot update. {p.inventory_id} stock would drop below zero."})
                
            if delta_stock != 0:
                new_serials = p.serial_numbers or []
                diff_serials = []
                if delta_stock > 0:
                    diff_serials = [s for s in new_serials if s not in old_serials]
                elif delta_stock < 0:
                    diff_serials = [s for s in old_serials if s not in new_serials]
                
                diff_serials = diff_serials[:abs(int(delta_stock))] if diff_serials else []
                while len(diff_serials) < abs(int(delta_stock)):
                    diff_serials.append("MISSING_" + generate_uuid()[:8])

                stock_adj_products.append(
                    StockAdjInventoryProductSchema(
                        inventory_id=p.inventory_id,
                        variant_id=p.variant_id,
                        batch_id=p.batch_id,
                        serialno_id=p.serialno_id,
                        serial_numbers=diff_serials,
                        stocks=abs(delta_stock),
                        type=StockAdjustmentTypesEnum.INCREMENT if delta_stock > 0 else StockAdjustmentTypesEnum.DECREMENT
                    )
                )
                
            inv_prod_toupdate.append({
                'b_id': p.inventory_id,
                'is_absolute': False,
                'stocks': delta_stock,
                'buy_price': p.buy_price,
                'sell_price': p.sell_price,
                'reorder_point': p.reorder_point,
                'is_active': True,
                'datas': None
            })
            
            purchase_inv_product_toadd.append(
                PurchaseInventoryProducts(
                    purchase_id=data.purchase_id,
                    inventory_id=p.inventory_id,
                    variant_id=p.variant_id,
                    batch_id=p.batch_id,
                    stocks=p.stocks,
                    stocks_before=current_inv_stock - old_stock, 
                    received_stocks=p.stocks,
                    sell_price=p.sell_price,
                    buy_price=p.buy_price
                )
            )
            
            readdb_products_toadd.append(
                PurchaseProduct(
                    inventory_id=p.inventory_id,
                    ui_id=raw_dict.get("ui_id", ""),
                    name=raw_dict.get("name", ""),
                    sell_price=p.sell_price,
                    buy_price=p.buy_price,
                    reorder_point=p.reorder_point,
                    stocks_before=current_inv_stock - old_stock,
                    stocks_added=p.stocks,
                    stocks_after=current_inv_stock + delta_stock,
                    total_amount=p.buy_price * p.stocks,
                    storage_location=p.storage_location or raw_dict.get("datas", {}).get("storage_location", ""),
                    gst=raw_dict.get("datas", {}).get("gst"),
                    variant=VariantInfo(
                        variant_id=p.variant_id,
                        variant_name=structured_variant.get(p.variant_id, {}).get("datas", {}).get("barcode", str(p.variant_id))
                    ) if p.variant_id else None,
                    batch=BatchInfo(
                        batch_id=p.batch_id,
                        batch_name=structured_batch.get(p.batch_id, {}).get("name", str(p.batch_id)),
                        mfg_date=structured_batch.get(p.batch_id, {}).get("manufacturing_date", ""),
                        exp_date=structured_batch.get(p.batch_id, {}).get("expiry_date", "")
                    ) if p.batch_id else None,
                    serial_info=SerialInfo(
                        serialno_id=p.serialno_id,
                        serial_numbers=p.serial_numbers
                    ) if p.serialno_id else None
                )
            )
            
        # Handle removed items
        for inv_id, old_stock in old_products_map.items():
            delta_stock = -old_stock
            raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inv_id))
            if raw_inventory:
                raw_dict = dict(raw_inventory)
                current_inv_stock = raw_dict.get("stocks", 0.0)
                if current_inv_stock + delta_stock < 0:
                    raise HTTPException(status_code=400, detail={"success": False, "msg": f"Stock for {inv_id} would drop below zero. Cannot edit."})
                
                stock_adj_products.append(
                    StockAdjInventoryProductSchema(
                        inventory_id=inv_id,
                        stocks=abs(delta_stock),
                        type=StockAdjustmentTypesEnum.DECREMENT
                    )
                )
                inv_prod_toupdate.append({
                    'b_id': inv_id,
                    'is_absolute': False,
                    'stocks': delta_stock,
                    'buy_price': raw_dict.get('buy_price'),
                    'sell_price': raw_dict.get('sell_price'),
                    'reorder_point': raw_dict.get('reorder_point'),
                    'is_active': True,
                    'datas': None
                })
        
        # Update Purchase DB
        data_toupdate=UpdatePurchaseDbSchema(
            datas=data.datas,
            shop_id=data.shop_id,
            id=data.purchase_id,
            type=data.type,
            paid_amount=data.paid_amount,
            calculations=data.calculations,
            additional_charges=data.additional_charges,
            purchase_view=True
        )
        await pur_repo_obj.update(data=data_toupdate)
        if purchase_inv_product_toadd:
            await pur_repo_obj.create_purchase_inv_bulk(data=purchase_inv_product_toadd)
            
        if inv_prod_toupdate:
            # We must update prices, but let StockAdjService handle the stock quantities.
            # So we set stocks=0 here so update_bulk only updates prices.
            price_only_toupdate = []
            for item in inv_prod_toupdate:
                new_item = item.copy()
                new_item['stocks'] = 0.0
                price_only_toupdate.append(new_item)
                
            await inv_repo_obj.update_bulk(datas=price_only_toupdate)
            
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            from infras.read_db.models.inventory_model import InventoryReadModel
            for item in price_only_toupdate:
                inv_id = item['b_id']
                raw_inv = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inv_id))
                if raw_inv:
                    await InventoryReadDbRepo.replace_inventory(InventoryReadModel(**dict(raw_inv)))
                    
                    
        if stock_adj_products:
            await StockAdjService(session=self.session).create_v2(
                can_update_stock=True,
                data=CreateStockAdjSchema(
                    shop_id=data.shop_id,
                    adjusted_date=datetime.utcnow().date(),
                    movement_type=StockAdjustmentMovementType.DIRECT,
                    description=f"Stock Adjusted via Purchase Edit {data.purchase_id}",
                    products=stock_adj_products
                )
            )

        # Read DB Purchase update
        total_cost = sum(p.total_amount for p in readdb_products_toadd)
        is_outstanding = data.paid_amount < total_cost
        
        old_purchase.invoice_no = invoice_no or old_purchase.invoice_no
        old_purchase.total_cost = total_cost
        old_purchase.total_items = len(data.products)
        old_purchase.total_quantity = sum([p.stocks for p in data.products])
        old_purchase.paid_amount = data.paid_amount
        old_purchase.payment_status = "outstanding" if is_outstanding else "completed"
        old_purchase.transport_charge = data.additional_charges.delivery_charge if data.additional_charges else 0.0
        old_purchase.other_charges = data.additional_charges.other_charge if data.additional_charges else 0.0
        old_purchase.calculations = data.calculations if data.calculations else {}
        old_purchase.calculations["is_edited"] = True
        old_purchase.products = readdb_products_toadd
        
        await PurchaseReadDbRepo.update_purchase(data.purchase_id, old_purchase)
        
        await _send_activity_log(
            shop_id=data.shop_id,
            action="EDIT",
            entity_id=data.purchase_id,
            description=f"Edited direct purchase {data.purchase_id}",
            changes=[{"field": "purchase", "before": "old_values", "after": "new_values"}]
        )
        return True

    async def create_direct_purchase_v2(self,data:CreatePurchaseSchema):
        from fastapi import HTTPException
        from infras.read_db.repos.purchase_repo import PurchaseReadDbRepo
        
        # Invoice Uniqueness Check
        invoice_no = data.datas.get("invoice_no") if data.datas else None
        if invoice_no:
            is_unique = await PurchaseReadDbRepo.check_invoice_uniqueness(
                shop_id=data.shop_id,
                supplier_id=data.supplier_id,
                invoice_no=invoice_no
            )
            if not is_unique:
                raise HTTPException(status_code=400, detail={"success": False, "msg": "Invoice number must be unique for this supplier."})

        inv_repo_obj=InventoryRepo(session=self.session)
        purchase_id=generate_uuid()
        structured_data:Dict[str,List[dict]]={}
        inventory_tocheck,variant_tocheck,batch_tocheck,serialno_tocheck=[],[],[],[]


        from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
        from core.utils.id_formatter import format_ui_id
        
        shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
        
        # Product Config
        inv_config = shop_config.get("product", {})
        prefix = inv_config.get("prefix", "PROD")
        start_from = inv_config.get("start_from", 1)
        
        # Purchase Config
        pur_config = shop_config.get("purchase", {})
        pur_prefix = pur_config.get("prefix", "PUR")
        pur_start_from = pur_config.get("start_from", 1)

        # STEP-1 DUPLICATION IDENTIFICATION
        for product in data.products:
            ERROR=None
            if product.inventory_id not in structured_data:
                structured_data[product.inventory_id]=[]
            else:
                values=structured_data[product.inventory_id]
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
        
            structured_data[product.inventory_id].append(product.model_dump())
            if product.inventory_id not in inventory_tocheck:
                inventory_tocheck.append(product.inventory_id)
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


        # DB UPDATE

        inv_prod_toupdate:List[dict]=[]
        variant_toupdate:List[dict]=[]
        batch_toupdate:dict={}
        serialno_toupdate:dict[str,List[str]]={}
        purchase_inv_product_toadd=[]
        batch_toadd:List[InventoryBatches]=[]
        serialno_toadd:List[InventorySerialNumbers]=[]

        readdb_products_toadd:List[PurchaseProduct]=[]

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
                stocks:int=val['stocks']
                variant_id:str=val.get('variant_id',None)
                batch_id:str=val.get('batch_id')
                serialno_id:str=val.get('serialno_id')
                buy_price:float=val['buy_price']
                sell_price:float=val['sell_price']
                reorder_point:int=val['reorder_point']
                batch:InventoryBatchSchema=InventoryBatchSchema(**val['batch']) if val.get('batch',None) else None
                serial_numbers:list[str]=val.get('serial_numbers',None)


                inv_stocks_before=structured_inv_result_data[inventory_id]['stocks'] if inventory_id in structured_inv_result_data else 0.0
                batch_stocks_before=structured_batch_result_data[batch_id]['stocks'] if batch_id in structured_batch_result_data else 0.0
                variant_stocks_before=structured_variant_result_data[variant_id]['stocks'] if variant_id in structured_variant_result_data else 0.0
                

                if has_variant and not variant_id:
                    ic("Variant id does not exists")
                    ERROR=True
                    break
                    
                if has_batch and (not batch_id and not batch):
                    ic("batch id doesnot exists")
                    ERROR=True
                    break

                if has_serialno and (not serialno_id and not serial_numbers):
                    ic("Serialno doesnot exists")
                    ERROR=True
                    break

                if serial_numbers and len(serial_numbers) != stocks:
                    ic("Serial number does not matches the stocks")
                    ERROR=True
                    break

                if variant_id:
                    variant_toupdate.append(
                        {
                            'b_id':variant_id,
                            'is_absolute':False,
                            'stocks':stocks,
                            'buy_price':buy_price,
                            'sell_price':sell_price,
                            'reorder_point':reorder_point,
                            'datas':val.get('datas', None)
                        }
                    )

                if batch_id:
                    batch_toupdate[batch_id]=stocks

                if not batch_id and batch:
                    batch_id=generate_uuid()
                    batch_toadd.append(
                        InventoryBatches(
                            id=batch_id,
                            shop_id=data.shop_id,
                            inventory_id=inventory_id,
                            variant_id=variant_id,
                            stocks=stocks,
                            expiry_date=batch.expiry_date,
                            manufacturing_date=batch.manufacturing_date,
                            name=batch.name,
                            datas={}
                        )
                    )

                if not serialno_id and serial_numbers:
                    if batch_id:
                        serialno_id = structured_batch_result_data.get(batch_id, {}).get("serialno_id")
                    elif variant_id:
                        serialno_id = structured_variant_result_data.get(variant_id, {}).get("serialno_id")
                    elif inventory_id:
                        serialno_id = structured_inv_result_data.get(inventory_id, {}).get("serialno_id")

                if serialno_id and serial_numbers:
                    serialno_toupdate.setdefault(serialno_id, []).extend(serial_numbers)

                if not serialno_id and serial_numbers:
                    serialno_id=generate_uuid()
                    serialno_toadd.append(
                        InventorySerialNumbers(
                            id=serialno_id,
                            shop_id=data.shop_id,
                            inventory_id=inventory_id,
                            variant_id=variant_id,
                            batch_id=batch_id,
                            serial_numbers=serial_numbers
                        )
                    )

                stocks_before=inv_stocks_before
                if has_variant:
                    stocks_before=variant_stocks_before
                if has_batch:
                    stocks_before=batch_stocks_before
                
                if has_variant and has_batch:
                    stocks_before=batch_stocks_before

                purchase_inv_product_toadd.append(
                    PurchaseInventoryProducts(
                        inventory_id=inventory_id,
                        purchase_id=purchase_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        stocks=stocks,
                        sell_price=sell_price,
                        buy_price=buy_price,
                        received_stocks=stocks,
                        stocks_before=stocks_before
                    )
                )

                stock_adj_products.append(
                    StockAdjInventoryProductOnlySchema(
                        inventory_id=inventory_id,
                        variant_id=variant_id,
                        batch_id=batch_id,
                        serialno_id=serialno_id,
                        serial_numbers=serial_numbers,
                        stocks=stocks,
                        stocks_before=stocks_before,
                        type=StockAdjustmentTypesEnum.INCREMENT
                    )
                )

                readdb_products_toadd.append(
                    PurchaseProduct(
                        inventory_id=inventory_id,
                        ui_id=structured_inv_result_data[inventory_id]['ui_id'],
                        name=structured_inv_result_data[inventory_id]['name'],
                        buy_price=buy_price,
                        sell_price=sell_price,
                        reorder_point=reorder_point,
                        stocks_before=stocks_before,
                        stocks_added=stocks,
                        stocks_after=stocks_before+stocks,
                        total_amount=stocks * buy_price,
                        variant=VariantInfo(
                            variant_id=variant_id,
                            variant_name=structured_variant_result_data.get(variant_id, {}).get('datas', {}).get('barcode', str(variant_id))
                        ) if variant_id else None,
                        batch=BatchInfo(
                            batch_id=batch_id,
                            batch_name=batch.name if batch else structured_batch_result_data.get(batch_id, {}).get('name') or str(batch_id),
                            mfg_date=batch.manufacturing_date if batch else structured_batch_result_data.get(batch_id, {}).get('manufacturing_date'),
                            exp_date=batch.expiry_date if batch else structured_batch_result_data.get(batch_id, {}).get('expiry_date')
                        ) if batch_id else None,
                        serial_info=SerialInfo(
                            serialno_id=serialno_id,
                            serial_numbers=serial_numbers
                        ) if serialno_id else None,
                        storage_location=(val.get('storage_location') or (val.get('datas', {}).get('storage_location', '') if val.get('datas') else '')) or structured_inv_result_data[inventory_id].get('datas', {}).get('storage_location', ''),
                        gst=structured_inv_result_data[inventory_id].get('datas', {}).get('gst')
                    )
                )

                inv_stocks+=stocks

            
            new_datas = structured_inv_result_data[inventory_id].get('datas', {}) or {}
            storage_val = val.get('storage_location') or (val.get('datas', {}).get('storage_location') if val.get('datas') else None)
            if storage_val:
                new_datas['storage_location'] = storage_val

            inv_prod_toupdate.append(
                {
                    'b_id':inventory_id,
                    'is_absolute':False,
                    'stocks':inv_stocks,
                    'buy_price':buy_price,
                    'sell_price':sell_price,
                    'reorder_point':reorder_point,
                    'is_active':True,
                    'datas':new_datas
                }
            )
            

            

            if ERROR:
                ic("Error Occured")
                return False
        
        
        pur_repo_obj=PurchaseRepo(session=self.session)

        raw_sequence = await pur_repo_obj.get_next_sequence(data.shop_id, pur_start_from)
        ui_id_str = format_ui_id(pur_prefix, pur_start_from, raw_sequence)

        data_toadd=CreatePurchaseDbSchema(
            **data.model_dump(mode='json'),
            id=purchase_id,
            ui_id=ui_id_str,
            purchase_view=True
        )

        res_ui_id=await pur_repo_obj.create(data=data_toadd)
        ic(res_ui_id)
        if res_ui_id:
            pur_inv_res=await pur_repo_obj.create_purchase_inv_bulk(data=purchase_inv_product_toadd)
            ic(pur_inv_res)
            await inv_repo_obj.create_batch_bulk(datas=batch_toadd)
            await inv_repo_obj.create_serialno_bulk(datas=serialno_toadd) 

            await inv_repo_obj.update_bulk(datas=inv_prod_toupdate)
            await inv_repo_obj.update_variant_bulk(datas=variant_toupdate)
            await inv_repo_obj.bulk_batch_qty_update(data=batch_toupdate,shop_id=data.shop_id)
            await inv_repo_obj.bulk_add_serialno(data=serialno_toupdate,shop_id=data.shop_id)
            
            # Sync stock increments and fields to Read DB
            from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
            from infras.read_db.models.inventory_model import InventoryReadModel
            from schemas.v1.request_schemas.inventory_schema import GetInventoryByIdSchema
            from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
            from core.utils.id_formatter import format_ui_id
            
            if inv_prod_toupdate:
                shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
                inv_config = shop_config.get("product", {})
                prefix = inv_config.get("prefix", "PROD")
                start_from = inv_config.get("start_from", 1)
                
                for item in inv_prod_toupdate:
                    inv_id = item['b_id']
                    raw_inventory = await inv_repo_obj.getby_id(GetInventoryByIdSchema(shop_id=data.shop_id, id=inv_id))
                    if raw_inventory:
                        raw_dict = dict(raw_inventory)
                        await InventoryReadDbRepo.replace_inventory(InventoryReadModel(**raw_dict))


            await StockAdjService(session=self.session).make_stock_adjustment(
                data=CreateStockAdjOnlySchema(
                    shop_id=data.shop_id,
                    movement_type=StockAdjustmentMovementType.DIRECT,
                    description=f"Purchase adjustment for purchase {purchase_id}",
                    products=stock_adj_products
                )
            )

            from infras.read_db.repos.purchase_repo import PurchaseStatsReadDbRepo
            from infras.read_db.repos.shopidconfig_repo import ShopIdConfigReadDbRepo
            from core.utils.id_formatter import format_ui_id
            from datetime import datetime
            
            # Formatted ID Generation
            shop_config = await ShopIdConfigReadDbRepo.get_config(data.shop_id)
            pur_config = shop_config.get("purchase", {})
            prefix = pur_config.get("prefix", "PUR")
            start_from = pur_config.get("start_from", 1)
            
            invoice_no = data.datas.get("invoice_no") if data.datas and data.datas.get("invoice_no") else res_ui_id
            total_cost = sum(p.total_amount for p in readdb_products_toadd)
            
            is_outstanding = data.paid_amount < total_cost
            outstanding_value = total_cost - data.paid_amount if is_outstanding else 0.0
            completed_value = total_cost if not is_outstanding else 0.0

            await PurchaseReadDbRepo.create_purchase(
                PurchaseReadModel(
                    purchase_id=purchase_id,
                    ui_id=ui_id_str,
                    shop_id=data.shop_id,
                    invoice_no=invoice_no,
                    purchase_date=datetime.utcnow(),
                    supplier=SupplierInfo(
                        supplier_id=data.supplier_id,
                        supplier_name=data.datas.get("supplier_name", "") if data.datas else ""
                    ),
                    total_cost=total_cost,
                    total_items=len(data.products),
                    total_quantity=sum([product.stocks for product in data.products]),
                    paid_amount=data.paid_amount,
                    payment_status="outstanding" if is_outstanding else "completed",
                    transport_charge=data.additional_charges.delivery_charge if data.additional_charges else 0.0,
                    other_charges=data.additional_charges.other_charge if data.additional_charges else 0.0,
                    calculations=data.calculations if data.calculations else {},
                    products=readdb_products_toadd
                )
            )

            await PurchaseStatsReadDbRepo.update_stats(
                shop_id=data.shop_id
            )

            product_count = len(data.products)
            await _send_activity_log(
                shop_id=data.shop_id,
                action="CREATE",
                entity_id=purchase_id,
                description=f"Created direct purchase v2 entry with {product_count} product(s)",
                changes=[
                    {"field": "type", "before": "", "after": "DIRECT_PURCHASE"},
                    {"field": "products", "before": "", "after": str(product_count)}
                ]
            )

        return True
                    
            


        
        

            

            




        

    
    