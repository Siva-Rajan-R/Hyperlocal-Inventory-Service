from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from ..models.product_model import Products,ProductBatches,ProductSerialNumbers,ProductVariants
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select,update,delete,func,or_,and_,String,case,cast,Text,literal,literal_column,text,bindparam,null
from sqlalchemy.dialects.postgresql import JSONB,ARRAY
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from schemas.v1.product_schemas.db_schemas import CreateProductBatchDbSchema,CreateProductDbSchema,CreateProductSerialnoDbSchema,CreateProductVariantDbSchema,UpdateProductBatchDbSchema,UpdateProductDbSchema,UpdateProductSerialnoDbSchema,UpdateProductVariantDbSchema,DeleteProductDbSchema
from schemas.v1.product_schemas.request_schemas import GetAllProductSchema,GetProductsById,GetProductsByShopId,VerifyCombinedSchema,GetBulkProductsById
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum
from core.utils.prod_inv_unit_builder import build_inventory_units
from ..models.inventory_model import InventoryPricings,InventoryStocks,InventoryStoragelocations,InventoryReorderPoint



class ProductRepo:
    def __init__(self,session: AsyncSession):
        self.session=session
        self.product_cols=(
            Products.id,Products.name,Products.category_id,Products.ui_id,Products.shop_id,
            Products.unit_id,Products.description,Products.type_infos,Products.is_active,Products.gst,
            Products.have_tracking,Products.created_at,Products.updated_at,Products.additional_infos
        )
        self.variant_cols = (
            ProductVariants.id,
            ProductVariants.product_id,
            ProductVariants.name,
            ProductVariants.additional_infos,
            ProductVariants.created_at,
            ProductVariants.updated_at,
        )
        self.batch_cols = (
            ProductBatches.id,
            ProductBatches.product_id,
            ProductBatches.variant_id,
            ProductBatches.name,
            ProductBatches.expiration_infos,
            ProductBatches.additional_infos,
            ProductBatches.created_at,
            ProductBatches.updated_at,
        )
        self.serialno_cols = (
            ProductSerialNumbers.id,
            ProductSerialNumbers.product_id,
            ProductSerialNumbers.variant_id,
            ProductSerialNumbers.batch_id,
            ProductSerialNumbers.name,
            ProductSerialNumbers.status,
            ProductSerialNumbers.additional_infos,
            ProductSerialNumbers.created_at,
            ProductSerialNumbers.updated_at,
        )
        self.invetory_pricing_cols = (
            InventoryPricings.id,
            InventoryPricings.product_id,
            InventoryPricings.variant_id,
            InventoryPricings.batch_id,
            InventoryPricings.buy_price,
            InventoryPricings.sell_price,
            InventoryPricings.additional_infos,
            InventoryPricings.created_at,
            InventoryPricings.updated_at,
        )
        self.inventory_stl_cols = (
            InventoryStoragelocations.id,
            InventoryStoragelocations.product_id,
            InventoryStoragelocations.variant_id,
            InventoryStoragelocations.batch_id,
            InventoryStoragelocations.name,
            InventoryStoragelocations.additional_infos,
            InventoryStoragelocations.created_at,
            InventoryStoragelocations.updated_at,
        )
        self.inventory_stocks_cols = (
            InventoryStocks.id,
            InventoryStocks.product_id,
            InventoryStocks.variant_id,
            InventoryStocks.batch_id,
            InventoryStocks.physical_stocks,
            InventoryStocks.reserved_stocks,
            InventoryStocks.available_stocks,
            InventoryStocks.additional_infos,
            InventoryStocks.created_at,
            InventoryStocks.updated_at,
        )
        self.inventory_rop_cols = (
            InventoryReorderPoint.id,
            InventoryReorderPoint.product_id,
            InventoryReorderPoint.variant_id,
            InventoryReorderPoint.batch_id,
            InventoryReorderPoint.reorder_point,
            InventoryReorderPoint.additional_infos,
            InventoryReorderPoint.created_at,
            InventoryReorderPoint.updated_at,
        )

    @start_db_transaction
    async def create_product(self,data:CreateProductDbSchema):
        stmt=(
            insert(
                Products
            )
            .values(
                **data.model_dump(mode="json")
            )
            .returning(*self.product_cols)
        )

        res=(await self.session.execute(stmt)).mappings().one_or_none()
        ic(res)
        return res
    
    
    @start_db_transaction
    async def create_bulk_variant(self,data: List[ProductVariants]):
        if data:
            self.session.add_all(data)
        return True
    
    @start_db_transaction
    async def create_bulk_batch(self,data: List[ProductBatches]):
        if data:
            self.session.add_all(data)
        return True
    
    @start_db_transaction
    async def create_bulk_selialno(self,data: List[ProductSerialNumbers]):
        if data:
            self.session.add_all(data)
        return True

    @start_db_transaction
    async def update_bulk_product(self,data:List[UpdateProductDbSchema]):
        if not data:
            return True
        final_data=[d.model_dump(mode="json",exclude_none=True,exclude_unset=True) for d in data]
        res=(
            await self.session.run_sync(
                lambda session:session.bulk_update_mappings(
                    Products,
                    final_data
                )
            )
        )

        ic(res)
        return True
    

    @start_db_transaction
    async def update_bulk_variant(self,data:List[UpdateProductVariantDbSchema]):
        if not data:
            return True
        final_data=[d.model_dump(mode="json",exclude_none=True,exclude_unset=True) for d in data]
        res=(
            await self.session.run_sync(
                lambda session:session.bulk_update_mappings(
                    ProductVariants,
                    final_data
                )
            )
        )

        ic(res)
        return True
    

    @start_db_transaction
    async def update_bulk_batch(self,data:List[UpdateProductBatchDbSchema]):
        if not data:
            return True
        final_data=[d.model_dump(mode="json",exclude_none=True,exclude_unset=True) for d in data]
        res=(
            await self.session.run_sync(
                lambda session:session.bulk_update_mappings(
                    ProductBatches,
                    final_data
                )
            )
        )

        ic(res)
        return True
    

    @start_db_transaction
    async def update_bulk_serialno(self,data:List[UpdateProductSerialnoDbSchema]):
        if not data:
            return True
        final_data=[d.model_dump(mode="json",exclude_none=True,exclude_unset=True) for d in data]
        res=(
            await self.session.run_sync(
                lambda session:session.bulk_update_mappings(
                    ProductSerialNumbers,
                    final_data
                )
            )
        )

        ic(res)
        return True

    
    @start_db_transaction
    async def delete_product(self,data:DeleteProductDbSchema):
        stmt=(
            delete(
                Products
            )
            .where(
                Products.id==data.id,
                Products.shop_id==data.shop_id
            )
            .returning(*self.product_cols)
        )

        res=(await self.session.execute(stmt)).scalar_one_or_none()
        ic(res)
        return res
    

    @start_db_transaction
    async def delete_bulk_serialno(self,data:List[str]):
        stmt=(
            delete(
                ProductSerialNumbers
            )
            .where(
                ProductSerialNumbers.id.in_(data)
            )
            .returning(*self.product_cols)
        )

        res=(await self.session.execute(stmt)).scalars().all()
        ic(res)
        return res
    

    async def get_products(self,data:GetAllProductSchema):
        offset=data.offset if data.offset>0 else 1
        cursor=(offset-1)*data.limit

        stmt = (
            select(Products)
            .options(
                selectinload(Products.variants).load_only(*self.variant_cols),

                selectinload(Products.batches).load_only(*self.batch_cols),


                selectinload(Products.stocks).load_only(*self.inventory_stocks_cols),

                selectinload(Products.pricings).load_only(*self.invetory_pricing_cols),

                selectinload(Products.storage_locations).load_only(*self.inventory_stl_cols),

                selectinload(Products.reorder_points).load_only(*self.inventory_rop_cols),
            )
            .limit(data.limit)
            .offset(cursor)
        )

        if data.include_serialno:
            stmt = stmt.options(
                selectinload(Products.serialnos).load_only(*self.serialno_cols)
            )

        res = (await self.session.execute(stmt)).scalars().all()
        ic(res)
        final_res=[]

        for product in res:
            res_toadd={
                "id": product.id,
                "shop_id": product.shop_id,
                "ui_id": product.ui_id,
                "name": product.name,
                "sku": product.sku,
                "barcode": product.barcode,
                "description": product.description,
                "category_id": product.category_id,
                "unit_id": product.unit_id,
                "is_active": product.is_active,
                "have_tracking": product.have_tracking,
                "created_at": product.created_at,
                "updated_at": product.updated_at,
                "type_infos": product.type_infos,
                "gst":product.gst
            }

            variant={}
            batches={}
            serialnos={}
            stocks={}
            rops={} 
            pricings={}
            stls={}

            for var in product.variants:
                variant[var['id']]=var
            
            for bat in product.batches:
                product_id=bat.product_id
                variant_id=bat.variant_id or ""
                combined_id=product_id+variant_id
                batches[combined_id]=bat

            if data.include_serialno:
                for serialno in product.serialnos:
                    product_id=serialno.product_id
                    variant_id=serialno.variant_id or ""
                    batch_id=serialno.batch_id or ""
                    combined_id=product_id+variant_id+batch_id
                    serialnos[combined_id]=serialno

            for stock in product.stocks:
                product_id=stock.product_id
                variant_id=stock.variant_id or ""
                batch_id=stock.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                stocks[combined_id]=stock
            
            for price in product.pricings:
                product_id=price.product_id
                variant_id=price.variant_id or ""
                batch_id=price.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                pricings[combined_id]=price
            
            for rop in product.reorder_points:
                product_id=rop.product_id
                variant_id=rop.variant_id or ""
                batch_id=rop.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                rops[combined_id]=rop
            
            for stl in product.storage_locations:
                product_id=stl.product_id
                variant_id=stl.variant_id or ""
                batch_id=stl.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                stls[combined_id]=stl


            
            
                
            if product.type_infos['has_variant']:
                variant_infos = {}

                for key, val in variant.items():
                    variant_id = key
                    product_id = val.product_id
                    combined_id = product_id + variant_id

                    variant_infos[variant_id] = {
                        "name": val.name
                    }

                    batch_id = ""

                    if product.type_infos["has_batch"]:
                        batch = batches[combined_id]

                        variant_infos[variant_id]["batch_infos"] = {
                            "id": batch.id,
                            "name": batch.name,
                            "expiry_date": batch.expiry_date,
                            "manufacturing_date": batch.manufacturing_date,
                        }

                        batch_id = batch.id

                    final_key = combined_id + batch_id

                    if product.type_infos["has_serialno"] and serialnos:
                        ic(serialnos)
                        variant_infos[variant_id]["serialno_infos"] = [
                            {
                                "id": sn.id,
                                "name": sn.name,
                            }
                            for sn in serialnos.get(final_key, [])
                        ]

                    stock = stocks.get(final_key)
                    variant_infos[variant_id]["stock_infos"] = (
                        {
                            "id": stock.id,
                            "physical_stocks": stock.physical_stocks,
                            "available_stocks": stock.available_stocks,
                            "reserved_stocks": stock.reserved_stocks,
                        }
                        if stock
                        else {}
                    )

                    pricing = pricings.get(final_key)
                    variant_infos[variant_id]["pricing_infos"] = (
                        {
                            "id": pricing.id,
                            "sell_price": pricing.sell_price,
                            "buy_price": pricing.buy_price,
                        }
                        if pricing
                        else {}
                    )

                    rop = rops.get(final_key)
                    variant_infos[variant_id]["reorder_point_infos"] = (
                        {
                            "id": rop.id,
                            "reorder_point": rop.reorder_point,
                        }
                        if rop
                        else {}
                    )

                    stl = stls.get(final_key)
                    variant_infos[variant_id]["storage_location_infos"] = (
                        {
                            "id": stl.id,
                            "storage_location": stl.name,
                        }
                        if stl
                        else {}
                    )

                res_toadd["variants"] = variant_infos

            else:
                batch_infos = {}
                serialno_infos = []
                stock_infos = {}
                pricing_infos = {}
                reorder_point_infos = {}
                storage_location_infos = {}

                variant_id = ""
                product_id = product.id
                combined_id = product_id + variant_id

                batch_id = ""

                if product.type_infos["has_batch"]:
                    batch = batches[combined_id]

                    batch_infos = {
                        "id": batch.id,
                        "name": batch.name,
                        "expiry_date": batch.expiry_date,
                        "manufacturing_date": batch.manufacturing_date,
                    }

                    batch_id = batch.id

                final_key = combined_id + batch_id

                if product.type_infos["has_serialno"] and serialnos:
                    serialno_infos = [
                        {
                            "id": sn.id,
                            "name": sn.name,
                        }
                        for sn in serialnos.get(final_key, [])
                    ]

                stock = stocks.get(final_key)
                if stock:
                    stock_infos = {
                        "id": stock.id,
                        "physical_stocks": stock.physical_stocks,
                        "available_stocks": stock.available_stocks,
                        "reserved_stocks": stock.reserved_stocks,
                    }

                pricing = pricings.get(final_key)
                if pricing:
                    pricing_infos = {
                        "id": pricing.id,
                        "sell_price": pricing.sell_price,
                        "buy_price": pricing.buy_price,
                    }

                rop = rops.get(final_key)
                if rop:
                    reorder_point_infos = {
                        "id": rop.id,
                        "reorder_point": rop.reorder_point,
                    }

                stl = stls.get(final_key)
                if stl:
                    storage_location_infos = {
                        "id": stl.id,
                        "storage_location": stl.name,
                    }

                res_toadd = {
                    **res_toadd,
                    "batch_infos": batch_infos,
                    "serialno_infos": serialno_infos,
                    "stock_infos": stock_infos,
                    "pricing_infos": pricing_infos,
                    "reorder_point_infos": reorder_point_infos,
                    "storage_location_infos": storage_location_infos,
                }

            final_res.append(res_toadd)
                
        return final_res
    

    async def get_products_by_shop_id(self,data:GetProductsByShopId):
        offset=data.offset if data.offset>0 else 1
        cursor=(offset-1)*data.limit

        stmt = (
            select(Products)
            .where(Products.shop_id==data.shop_id)
            .options(
                selectinload(Products.variants).load_only(*self.variant_cols),

                selectinload(Products.batches).load_only(*self.batch_cols),

                selectinload(Products.stocks).load_only(*self.inventory_stocks_cols),

                selectinload(Products.pricings).load_only(*self.invetory_pricing_cols),

                selectinload(Products.storage_locations).load_only(*self.inventory_stl_cols),

                selectinload(Products.reorder_points).load_only(*self.inventory_rop_cols),
            )
            .limit(data.limit)
            .offset(cursor)
        )

        if data.include_serialno:
            stmt = stmt.options(
                selectinload(Products.serialnos).load_only(*self.serialno_cols)
            )

        res=(await self.session.execute(stmt)).scalars().all()
        ic(res)
        final_res=[]

        for product in res:
            res_toadd={
                "id": product.id,
                "shop_id": product.shop_id,
                "ui_id": product.ui_id,
                "name": product.name,
                "sku": product.sku,
                "barcode": product.barcode,
                "description": product.description,
                "category_id": product.category_id,
                "unit_id": product.unit_id,
                "is_active": product.is_active,
                "have_tracking": product.have_tracking,
                "created_at": product.created_at,
                "updated_at": product.updated_at,
                "type_infos": product.type_infos,
                "gst":product.gst
            }

            variant={}
            batches={}
            serialnos={}
            stocks={}
            rops={} 
            pricings={}
            stls={}

            for var in product.variants:
                variant[var['id']]=var
            
            for bat in product.batches:
                product_id=bat.product_id
                variant_id=bat.variant_id or ""
                combined_id=product_id+variant_id
                batches[combined_id]=bat

            if data.include_serialno:
                for serialno in product.serialnos:
                    product_id=serialno.product_id
                    variant_id=serialno.variant_id or ""
                    batch_id=serialno.batch_id or ""
                    combined_id=product_id+variant_id+batch_id
                    serialnos[combined_id]=serialno

            for stock in product.stocks:
                product_id=stock.product_id
                variant_id=stock.variant_id or ""
                batch_id=stock.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                stocks[combined_id]=stock
            
            for price in product.pricings:
                product_id=price.product_id
                variant_id=price.variant_id or ""
                batch_id=price.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                pricings[combined_id]=price
            
            for rop in product.reorder_points:
                product_id=rop.product_id
                variant_id=rop.variant_id or ""
                batch_id=rop.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                rops[combined_id]=rop
            
            for stl in product.storage_locations:
                product_id=stl.product_id
                variant_id=stl.variant_id or ""
                batch_id=stl.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                stls[combined_id]=stl


            
            
                
            if product.type_infos['has_variant']:
                variant_infos = {}

                for key, val in variant.items():
                    variant_id = key
                    product_id = val.product_id
                    combined_id = product_id + variant_id

                    variant_infos[variant_id] = {
                        "name": val.name
                    }

                    batch_id = ""

                    if product.type_infos["has_batch"]:
                        batch = batches[combined_id]

                        variant_infos[variant_id]["batch_infos"] = {
                            "id": batch.id,
                            "name": batch.name,
                            "expiry_date": batch.expiry_date,
                            "manufacturing_date": batch.manufacturing_date,
                        }

                        batch_id = batch.id

                    final_key = combined_id + batch_id

                    if product.type_infos["has_serialno"] and serialnos:
                        ic(serialnos)
                        variant_infos[variant_id]["serialno_infos"] = [
                            {
                                "id": sn.id,
                                "name": sn.name,
                            }
                            for sn in serialnos.get(final_key, [])
                        ]

                    stock = stocks.get(final_key)
                    variant_infos[variant_id]["stock_infos"] = (
                        {
                            "id": stock.id,
                            "physical_stocks": stock.physical_stocks,
                            "available_stocks": stock.available_stocks,
                            "reserved_stocks": stock.reserved_stocks,
                        }
                        if stock
                        else {}
                    )

                    pricing = pricings.get(final_key)
                    variant_infos[variant_id]["pricing_infos"] = (
                        {
                            "id": pricing.id,
                            "sell_price": pricing.sell_price,
                            "buy_price": pricing.buy_price,
                        }
                        if pricing
                        else {}
                    )

                    rop = rops.get(final_key)
                    variant_infos[variant_id]["reorder_point_infos"] = (
                        {
                            "id": rop.id,
                            "reorder_point": rop.reorder_point,
                        }
                        if rop
                        else {}
                    )

                    stl = stls.get(final_key)
                    variant_infos[variant_id]["storage_location_infos"] = (
                        {
                            "id": stl.id,
                            "storage_location": stl.name,
                        }
                        if stl
                        else {}
                    )

                res_toadd["variants"] = variant_infos

            else:
                batch_infos = {}
                serialno_infos = []
                stock_infos = {}
                pricing_infos = {}
                reorder_point_infos = {}
                storage_location_infos = {}

                variant_id = ""
                product_id = product.id
                combined_id = product_id + variant_id

                batch_id = ""

                if product.type_infos["has_batch"]:
                    batch = batches[combined_id]

                    batch_infos = {
                        "id": batch.id,
                        "name": batch.name,
                        "expiry_date": batch.expiry_date,
                        "manufacturing_date": batch.manufacturing_date,
                    }

                    batch_id = batch.id

                final_key = combined_id + batch_id

                if product.type_infos["has_serialno"] and serialnos:
                    serialno_infos = [
                        {
                            "id": sn.id,
                            "name": sn.name,
                        }
                        for sn in serialnos.get(final_key, [])
                    ]

                stock = stocks.get(final_key)
                if stock:
                    stock_infos = {
                        "id": stock.id,
                        "physical_stocks": stock.physical_stocks,
                        "available_stocks": stock.available_stocks,
                        "reserved_stocks": stock.reserved_stocks,
                    }

                pricing = pricings.get(final_key)
                if pricing:
                    pricing_infos = {
                        "id": pricing.id,
                        "sell_price": pricing.sell_price,
                        "buy_price": pricing.buy_price,
                    }

                rop = rops.get(final_key)
                if rop:
                    reorder_point_infos = {
                        "id": rop.id,
                        "reorder_point": rop.reorder_point,
                    }

                stl = stls.get(final_key)
                if stl:
                    storage_location_infos = {
                        "id": stl.id,
                        "storage_location": stl.name,
                    }

                res_toadd = {
                    **res_toadd,
                    "batch_infos": batch_infos,
                    "serialno_infos": serialno_infos,
                    "stock_infos": stock_infos,
                    "pricing_infos": pricing_infos,
                    "reorder_point_infos": reorder_point_infos,
                    "storage_location_infos": storage_location_infos,
                }

            final_res.append(res_toadd)
                
        return final_res
    

    async def get_products_by_id(self,data:GetProductsById):
        stmt = (
            select(Products)
            .where(Products.shop_id==data.shop_id,Products.id==data.id)
            .options(
                selectinload(Products.variants).load_only(*self.variant_cols),

                selectinload(Products.batches).load_only(*self.batch_cols),

                selectinload(Products.stocks).load_only(*self.inventory_stocks_cols),

                selectinload(Products.pricings).load_only(*self.invetory_pricing_cols),

                selectinload(Products.storage_locations).load_only(*self.inventory_stl_cols),

                selectinload(Products.reorder_points).load_only(*self.inventory_rop_cols),
            )
        )

        if data.include_serialno:
            stmt=stmt.options(selectinload(Products.serialnos).load_only(*self.serialno_cols))

        res=(await self.session.execute(stmt)).scalars().one_or_none()
        ic(res)
        if not res:
            return None
            
        final_res=[]

        for product in [res]:
            final_res.append(
                {
                    "id": product.id,
                    "shop_id": product.shop_id,
                    "ui_id": product.ui_id,
                    "name": product.name,
                    "sku": product.sku,
                    "gst":product.gst,
                    "barcode": product.barcode,
                    "description": product.description,
                    "category_id": product.category_id,
                    "unit_id": product.unit_id,
                    "gst":product.gst,
                    "is_active": product.is_active,
                    "have_tracking": product.have_tracking,
                    "created_at": product.created_at,
                    "updated_at": product.updated_at,
                    "type_infos": product.type_infos,
                    "inventory_units": build_inventory_units(
                        product,
                        include_serialno=data.include_serialno
                    ),
                }
            )

        return final_res[0]


    async def get_bulk_products_by_id(self,data:GetBulkProductsById):
        stmt = (
            select(Products)
            .where(Products.shop_id==data.shop_id,Products.id.in_(data.id))
            .options(
                selectinload(Products.variants).load_only(*self.variant_cols),

                selectinload(Products.batches).load_only(*self.batch_cols),

                selectinload(Products.stocks).load_only(*self.inventory_stocks_cols),

                selectinload(Products.pricings).load_only(*self.invetory_pricing_cols),

                selectinload(Products.storage_locations).load_only(*self.inventory_stl_cols),

                selectinload(Products.reorder_points).load_only(*self.inventory_rop_cols),
            )
        )

        if data.include_serialno:
            stmt=stmt.options(selectinload(Products.serialnos).load_only(*self.serialno_cols))

        res=(await self.session.execute(stmt)).scalars().all()
        ic(res)
        final_res=[]

        for product in res:
            res_toadd={
                "id": product.id,
                "shop_id": product.shop_id,
                "ui_id": product.ui_id,
                "name": product.name,
                "sku": product.sku,
                "barcode": product.barcode,
                "description": product.description,
                "category_id": product.category_id,
                "unit_id": product.unit_id,
                "is_active": product.is_active,
                "have_tracking": product.have_tracking,
                "created_at": product.created_at,
                "updated_at": product.updated_at,
                "type_infos": product.type_infos,
                "gst":product.gst
            }

            variant={}
            batches={}
            serialnos={}
            stocks={}
            rops={} 
            pricings={}
            stls={}

            for var in product.variants:
                variant[var['id']]=var
            
            for bat in product.batches:
                product_id=bat.product_id
                variant_id=bat.variant_id or ""
                combined_id=product_id+variant_id
                batches[combined_id]=bat

            if data.include_serialno:
                for serialno in product.serialnos:
                    product_id=serialno.product_id
                    variant_id=serialno.variant_id or ""
                    batch_id=serialno.batch_id or ""
                    combined_id=product_id+variant_id+batch_id
                    serialnos[combined_id]=serialno

            for stock in product.stocks:
                product_id=stock.product_id
                variant_id=stock.variant_id or ""
                batch_id=stock.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                stocks[combined_id]=stock
            
            for price in product.pricings:
                product_id=price.product_id
                variant_id=price.variant_id or ""
                batch_id=price.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                pricings[combined_id]=price
            
            for rop in product.reorder_points:
                product_id=rop.product_id
                variant_id=rop.variant_id or ""
                batch_id=rop.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                rops[combined_id]=rop
            
            for stl in product.storage_locations:
                product_id=stl.product_id
                variant_id=stl.variant_id or ""
                batch_id=stl.batch_id or ""
                combined_id=product_id+variant_id+batch_id
                stls[combined_id]=stl


            
            
                
            if product.type_infos['has_variant']:
                variant_infos = {}

                for key, val in variant.items():
                    variant_id = key
                    product_id = val.product_id
                    combined_id = product_id + variant_id

                    variant_infos[variant_id] = {
                        "name": val.name
                    }

                    batch_id = ""

                    if product.type_infos['has_batch'] and product.type_infos['has_serialno']:
                        batch = batches[combined_id]
                        batch_id = batch.id
                        final_key = combined_id + batch_id
                        variant_infos[variant_id]["batch_infos"] = {
                            "id": batch.id,
                            "name": batch.name,
                            "expiry_date": batch.expiry_date,
                            "manufacturing_date": batch.manufacturing_date,
                        }

                        serialno_infos = [
                            {
                                "id": sn.id,
                                "name": sn.name,
                            }
                            for sn in serialnos.get(final_key, [])
                        ]

                        variant_infos[variant_id]["batch_infos"]['serialno_infos']=serialno_infos

                    if product.type_infos["has_batch"] and not product.type_infos['has_serialno']:
                        batch = batches[combined_id]

                        variant_infos[variant_id]["batch_infos"] = {
                            "id": batch.id,
                            "name": batch.name,
                            "expiry_date": batch.expiry_date,
                            "manufacturing_date": batch.manufacturing_date,
                        }

                        batch_id = batch.id

                    final_key = combined_id + batch_id

                    if product.type_infos["has_serialno"] and serialnos and not product.type_infos["has_batch"]:
                        ic(serialnos)
                        variant_infos[variant_id]["serialno_infos"] = [
                            {
                                "id": sn.id,
                                "name": sn.name,
                            }
                            for sn in serialnos.get(final_key, [])
                        ]

                    stock = stocks.get(final_key)
                    if stock and product.type_infos["has_batch"]:
                        variant_infos[variant_id]['batch_infos'] = {
                            "id": stock.id,
                            "physical_stocks": stock.physical_stocks,
                            "available_stocks": stock.available_stocks,
                            "reserved_stocks": stock.reserved_stocks,
                        }
                        batch_infos['stock_infos']=stock_infos

                    if stock and not product.type_infos["has_batch"]:  
                        variant_infos[variant_id]["stock_infos"] = (
                            {
                                "id": stock.id,
                                "physical_stocks": stock.physical_stocks,
                                "available_stocks": stock.available_stocks,
                                "reserved_stocks": stock.reserved_stocks,
                            }
                        )

                    pricing = pricings.get(final_key)
                    variant_infos[variant_id]["pricing_infos"] = (
                        {
                            "id": pricing.id,
                            "sell_price": pricing.sell_price,
                            "buy_price": pricing.buy_price,
                        }
                        if pricing
                        else {}
                    )

                    rop = rops.get(final_key)
                    variant_infos[variant_id]["reorder_point_infos"] = (
                        {
                            "id": rop.id,
                            "reorder_point": rop.reorder_point,
                        }
                        if rop
                        else {}
                    )

                    stl = stls.get(final_key)
                    variant_infos[variant_id]["storage_location_infos"] = (
                        {
                            "id": stl.id,
                            "storage_location": stl.name,
                        }
                        if stl
                        else {}
                    )

                res_toadd["variants"] = variant_infos

            else:
                batch_infos = {}
                serialno_infos = []
                stock_infos = {}
                pricing_infos = {}
                reorder_point_infos = {}
                storage_location_infos = {}

                variant_id = ""
                product_id = product.id
                combined_id = product_id + variant_id

                batch_id = ""

                if product.type_infos['has_batch'] and product.type_infos['has_serialno']:
                    batch = batches[combined_id]
                    batch_id = batch.id
                    final_key = combined_id + batch_id
                    batch_infos = {
                        "id": batch.id,
                        "name": batch.name,
                        "expiry_date": batch.expiry_date,
                        "manufacturing_date": batch.manufacturing_date,
                    }

                    serialno_infos = [
                        {
                            "id": sn.id,
                            "name": sn.name,
                        }
                        for sn in serialnos.get(final_key, [])
                    ]

                    batch_infos['serialno_infos']=serialno_infos

                    
                    

                if product.type_infos["has_batch"] and not product.type_infos['has_serialno']:
                    batch = batches[combined_id]

                    batch_infos = {
                        "id": batch.id,
                        "name": batch.name,
                        "expiry_date": batch.expiry_date,
                        "manufacturing_date": batch.manufacturing_date,
                    }

                    batch_id = batch.id

                final_key = combined_id + batch_id

                if product.type_infos["has_serialno"] and serialnos and not product.type_infos["has_batch"]:
                    serialno_infos = [
                        {
                            "id": sn.id,
                            "name": sn.name,
                        }
                        for sn in serialnos.get(final_key, [])
                    ]

                stock = stocks.get(final_key)
                if stock and product.type_infos["has_batch"]:
                    stock_infos = {
                        "id": stock.id,
                        "physical_stocks": stock.physical_stocks,
                        "available_stocks": stock.available_stocks,
                        "reserved_stocks": stock.reserved_stocks,
                    }
                    batch_infos['stock_infos']=stock_infos

                if stock and not product.type_infos["has_batch"]:
                    stock_infos = {
                        "id": stock.id,
                        "physical_stocks": stock.physical_stocks,
                        "available_stocks": stock.available_stocks,
                        "reserved_stocks": stock.reserved_stocks,
                    }

                pricing = pricings.get(final_key)
                if pricing:
                    pricing_infos = {
                        "id": pricing.id,
                        "sell_price": pricing.sell_price,
                        "buy_price": pricing.buy_price,
                    }

                rop = rops.get(final_key)
                if rop:
                    reorder_point_infos = {
                        "id": rop.id,
                        "reorder_point": rop.reorder_point,
                    }

                stl = stls.get(final_key)
                if stl:
                    storage_location_infos = {
                        "id": stl.id,
                        "storage_location": stl.name,
                    }

                res_toadd = {
                    **res_toadd,
                    "batch_infos": batch_infos,
                    "serialno_infos": serialno_infos,
                    "stock_infos": stock_infos,
                    "pricing_infos": pricing_infos,
                    "reorder_point_infos": reorder_point_infos,
                    "storage_location_infos": storage_location_infos,
                }

            final_res.append(res_toadd)
                
        return final_res
    

    
    


    async def verify_bulk_product(self,data: List[str]):
        stmt=(
            select(
                *self.product_cols
            )
            .where(
                Products.id.in_(data)
            )
        )

        res=(await self.session.execute(stmt)).mappings().all()
        ic(res)
        return res
    

    async def verify_bulk_variant(self,data: List[str]):
        stmt=(
            select(
                ProductVariants.id
            )
            .where(
                ProductVariants.id.in_(data)
            )
        )

        res=(await self.session.execute(stmt)).scalars().all()
        ic(res)
        return res
    

    async def verify_bulk_batch(self,data: List[str]):
        stmt=(
            select(
                ProductBatches.id
            )
            .where(
                ProductBatches.id.in_(data)
            )
        )

        res=(await self.session.execute(stmt)).scalars().all()
        ic(res)
        return res
    
    async def verify_bulk_serialno(self,data: List[str]):
        stmt=(
            select(
                ProductSerialNumbers.id
            )
            .where(
                ProductSerialNumbers.id.in_(data)
            )
        )

        res=(await self.session.execute(stmt)).scalars().all()
        ic(res)
        return res
    

    async def verify_bulk_serialno_name(self,data: List[dict]):
        stmt=(
            select(
                ProductSerialNumbers.id
            )
            .where(
                ProductSerialNumbers.shop_id==bindparam("shop_id"),
                ProductSerialNumbers.product_id==bindparam("product_id"),
                ProductSerialNumbers.variant_id.is_not_distinct_from(bindparam("variant_id")),
                ProductSerialNumbers.batch_id.is_not_distinct_from(bindparam("batch_id")),
                ProductSerialNumbers.name.in_(bindparam("names",expanding=True))
            )
        )

        res=(
            await self.session.execute(
                stmt,
                [
                    {
                        'shop_id':d['shop_id'],
                        'product_id':d['product_id'],
                        'variant_id':d['variant_id'],
                        'batch_id':d['batch_name'],
                        'names':d['names']
                    }
                    for d in data
                ]
            )
        )
        ic(res)
        return res
    

    async def verify_combined(self,data:VerifyCombinedSchema):
        products=[]
        variants=[]
        batches=[]
        serialnos=[]
        
        if data.products:
           res=await self.verify_bulk_product(data=data.products)
           ic(res)
           products.extend(res)

        if data.variants:
           res=await self.verify_bulk_variant(data=data.variants)
           ic(res)
           variants.extend(res)

        if data.batches:
           res=await self.verify_bulk_batch(data=data.batches)
           ic(res)
           batches.extend(res)

        if data.serialnos:
            res=await self.verify_bulk_serialno(data=data.serialnos)
            ic(res)
            serialnos.extend(res)
        


        return {
            "products":products,
            "variants":variants,
            "batches":batches,
            "serialnos":serialnos
        } 
    
    
        