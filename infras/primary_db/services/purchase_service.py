from models.service_models.base_service_model import BaseServiceModel
from ..repos.purchase_repo import PurchaseRepo
from ..services.stock_adj_service import StockAdjService
from core.data_formats.enums.stock_adj_enums import StockAdjustmentMovementType,StockAdjustmentTypesEnum
from typing import Optional,List
from ..models.purchase_model import Purchase,PurchaseInventoryProducts
from schemas.v1.request_schemas.stock_adj_schema import CreateStockAdjSchema,StockAdjInventoryProductSchema
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
from typing import Union
from datetime import date


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
                            'reorder_point':requested_data['reorder_point']
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
                        'is_active':True
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

        data_toadd=CreatePurchaseDbSchema(
            **data.model_dump(mode='json'),
            id=purchase_id,
            purchase_view=purchase_view
        )
        pur_repo_obj=PurchaseRepo(session=self.session)
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

            NEXT=True


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
        return await self.purchase_repo_obj.delete(id=id,shop_id=shop_id)

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

    async def search(self, query, limit = 5):
        return await self.purchase_repo_obj.search(query=query,limit=limit)
    

    async def create_direct_purchase(self,data:CreatePurchaseSchema):

        inv_repo_obj=InventoryRepo(session=self.session)
        purchase_id=generate_uuid()
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

            for formated_data in checking_foramtted_data[inv_prod_id]:
                ic(formated_data)
                stocks:int=formated_data['stocks']
                variant_id:str=formated_data.get('variant_id',None)
                batch_id:str=formated_data.get('batch_id')
                serialno_id:str=formated_data.get('serialno_id')
                buy_price:float=formated_data['buy_price']
                sell_price:float=formated_data['sell_price']
                reorder_point:int=formated_data['reorder_point']
                batch:InventoryBatchSchema=InventoryBatchSchema(**formated_data['batch']) if formated_data.get('batch',None) else None
                serial_numbers:list[str]=formated_data.get('serial_numbers',None)
                inv_stocks+=stocks

                ic(formated_data,stocks,variant_id,batch_id,serialno_id,buy_price,sell_price,batch,serial_numbers)

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
                            'reorder_point':reorder_point
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

                if serialno_id:
                    serialno_toupdate[serialno_id]=serial_numbers

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
                        stocks_before=result['stocks']
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
                    'is_active':True
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

        res=await pur_repo_obj.create(data=data_toadd)
        ic("hello p")
        ic(res)
        if res:
            pur_inv_res=await pur_repo_obj.create_purchase_inv_bulk(data=purchase_inv_product_toadd)
            ic(pur_inv_res)
            await inv_repo_obj.update_bulk(datas=inv_prod_toupdate)
            await inv_repo_obj.update_variant_bulk(datas=variant_toupdate)
            await inv_repo_obj.bulk_batch_qty_update(data=batch_toupdate,shop_id=data.shop_id)
            await inv_repo_obj.bulk_add_serialno(data=serialno_toupdate,shop_id=data.shop_id)

            await inv_repo_obj.create_batch_bulk(datas=batch_toadd)
            await inv_repo_obj.create_serialno_bulk(datas=serialno_toadd)


            await StockAdjService(session=self.session).create(
                can_update_stock=False,
                data=CreateStockAdjSchema(
                    shop_id=data.shop_id,
                    adjusted_date=date.today(),
                    movement_type=StockAdjustmentMovementType.DIRECT,
                    description="Stock Increased via purchase",
                    products=stock_adj_products
                )
            )


            ic("all of them updated successfully")

            return True
        return False
        

            

            




        

    
    