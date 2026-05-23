from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case,literal,literal_column,bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from schemas.v1.request_schemas.stock_adj_schema import GetStockAdjByShopIdSchema,GetAllStockAdjSchema,GetStockAdjByIdSchema,GetStockAdjByInventoryIdSchema
from ..models.inventory_model import StockAdjustments,StockAdjustmentInventoryProducts,Inventory,InventoryBatches,InventorySerialNumbers,InventoryVariants
from schemas.v1.db_schemas.stock_adj_schema import CreateStockAdjDbSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic

from sqlalchemy import (
    select,
    func,
    and_,
    or_
)

# =========================================================
# ALIASES
# =========================================================

sa = StockAdjustments
sap = StockAdjustmentInventoryProducts
i = Inventory
v = InventoryVariants
b = InventoryBatches
s = InventorySerialNumbers


# =========================================================
# STOCK AGG
# =========================================================

sap_agg = (
    select(
        sap.stockadjustment_id,
        sap.inventory_id,
        sap.variant_id,
        sap.batch_id,

        sap.stocks_before,
        sap.type,

        func.sum(sap.stocks).label("stocks")

    )
    .group_by(
        sap.stockadjustment_id,
        sap.inventory_id,
        sap.variant_id,
        sap.batch_id,
        sap.stocks_before,
        sap.type
    )
).subquery()


# =========================================================
# SERIALS INSIDE BATCH
# =========================================================

serial_subq = (
    select(
        func.jsonb_agg(
            func.distinct(
                func.jsonb_build_object(
                    "id", s.id,
                    "serial_numbers",
                    s.serial_numbers
                )
            )
        )
    )
    .where(
        s.batch_id == b.id
    )
    .scalar_subquery()
)


# =========================================================
# DIRECT SERIALS
# =========================================================

direct_serial_subq = (
    select(
        sap_agg.c.stockadjustment_id,
        sap_agg.c.inventory_id,
        sap_agg.c.variant_id,

        func.jsonb_agg(
            func.distinct(
                func.jsonb_build_object(
                    "id", s.id,
                    "serial_numbers",
                    s.serial_numbers
                )
            )
        ).label("serials")
    )
    .join(
        s,
        s.inventory_id ==
        sap_agg.c.inventory_id
    )
    .where(
        sap_agg.c.batch_id.is_(None)
    )
    .group_by(
        sap_agg.c.stockadjustment_id,
        sap_agg.c.inventory_id,
        sap_agg.c.variant_id
    )
).subquery()


# =========================================================
# BATCH SUBQUERY
# =========================================================

batch_subq = (
    select(
        sap_agg.c.stockadjustment_id,
        sap_agg.c.inventory_id,
        sap_agg.c.variant_id,

        func.jsonb_agg(
            func.distinct(
                func.jsonb_build_object(
                    "id", b.id,
                    "name", b.name,

                    "stocks",
                    sap_agg.c.stocks,

                    "expiry_date",
                    b.expiry_date,

                    "manufacturing_date",
                    b.manufacturing_date,

                    "serials",
                    serial_subq
                )
            )
        ).label("batches")
    )
    .join(
        b,
        b.id ==
        sap_agg.c.batch_id
    )
    .group_by(
        sap_agg.c.stockadjustment_id,
        sap_agg.c.inventory_id,
        sap_agg.c.variant_id
    )
).subquery()


# =========================================================
# VARIANTS
# =========================================================

variant_subq = (
    select(
        sap_agg.c.stockadjustment_id,
        sap_agg.c.inventory_id,
        sap_agg.c.variant_id,

        func.jsonb_agg(
            func.distinct(
                func.jsonb_build_object(
                    "id", v.id,
                    "name", v.name,

                    "stocks",
                    sap_agg.c.stocks,

                    "batches",
                    batch_subq.c.batches,

                    "serials",
                    direct_serial_subq.c.serials
                )
            )
        ).label("variants")
    )

    .join(
        v,
        v.id ==
        sap_agg.c.variant_id
    )

    .outerjoin(
        batch_subq,
        and_(
            batch_subq.c.stockadjustment_id ==
            sap_agg.c.stockadjustment_id,

            batch_subq.c.inventory_id ==
            sap_agg.c.inventory_id,

            batch_subq.c.variant_id ==
            sap_agg.c.variant_id
        )
    )

    .outerjoin(
        direct_serial_subq,
        and_(
            direct_serial_subq.c.stockadjustment_id ==
            sap_agg.c.stockadjustment_id,

            direct_serial_subq.c.inventory_id ==
            sap_agg.c.inventory_id,

            or_(
                direct_serial_subq.c.variant_id ==
                sap_agg.c.variant_id,

                and_(
                    direct_serial_subq.c.variant_id.is_(None),
                    sap_agg.c.variant_id.is_(None)
                )
            )
        )
    )

    .group_by(
        sap_agg.c.stockadjustment_id,
        sap_agg.c.inventory_id,
        sap_agg.c.variant_id,

        batch_subq.c.batches,
        direct_serial_subq.c.serials
    )
).subquery()


# =========================================================
# PRODUCT SUBQUERY
# =========================================================

product_subq = (
    select(
        sap_agg.c.stockadjustment_id,

        i.id.label("id"),
        i.ui_id.label("ui_id"),
        i.sequence_id.label("sequence_id"),

        i.name.label("name"),
        i.description.label("description"),
        i.barcode.label("barcode"),
        i.category.label("category"),

        i.has_variant,
        i.has_batch,
        i.has_serialno,

        sap_agg.c.stocks,
        sap_agg.c.stocks_before,
        sap_agg.c.type,

        variant_subq.c.variants,
        batch_subq.c.batches,
        direct_serial_subq.c.serials
    )

    .join(
        i,
        i.id ==
        sap_agg.c.inventory_id
    )

    .outerjoin(
        variant_subq,
        and_(
            variant_subq.c.stockadjustment_id ==
            sap_agg.c.stockadjustment_id,

            variant_subq.c.inventory_id ==
            sap_agg.c.inventory_id,

            or_(
                variant_subq.c.variant_id ==
                sap_agg.c.variant_id,

                and_(
                    variant_subq.c.variant_id.is_(None),
                    sap_agg.c.variant_id.is_(None)
                )
            )
        )
    )

    .outerjoin(
        batch_subq,
        and_(
            batch_subq.c.stockadjustment_id ==
            sap_agg.c.stockadjustment_id,

            batch_subq.c.inventory_id ==
            sap_agg.c.inventory_id,

            or_(
                batch_subq.c.variant_id ==
                sap_agg.c.variant_id,

                and_(
                    batch_subq.c.variant_id.is_(None),
                    sap_agg.c.variant_id.is_(None)
                )
            )
        )
    )

    .outerjoin(
        direct_serial_subq,
        and_(
            direct_serial_subq.c.stockadjustment_id ==
            sap_agg.c.stockadjustment_id,

            direct_serial_subq.c.inventory_id ==
            sap_agg.c.inventory_id,

            or_(
                direct_serial_subq.c.variant_id ==
                sap_agg.c.variant_id,

                and_(
                    direct_serial_subq.c.variant_id.is_(None),
                    sap_agg.c.variant_id.is_(None)
                )
            )
        )
    )

).subquery()


# =========================================================
# PRODUCTS AGG
# =========================================================

products_agg = func.jsonb_agg(
    func.distinct(
        func.jsonb_build_object(

            "id", product_subq.c.id,
            "ui_id", product_subq.c.ui_id,
            "sequence_id", product_subq.c.sequence_id,

            "name", product_subq.c.name,
            "description", product_subq.c.description,
            "barcode", product_subq.c.barcode,
            "category", product_subq.c.category,

            "has_variant", product_subq.c.has_variant,
            "has_batch", product_subq.c.has_batch,
            "has_serialno", product_subq.c.has_serialno,

            "stocks",
            product_subq.c.stocks,

            "stocks_before",
            product_subq.c.stocks_before,

            "type",
            product_subq.c.type,

            "variants",
            product_subq.c.variants,

            "batches",
            product_subq.c.batches,

            "serials",
            product_subq.c.serials
        )
    )
)


# =========================================================
# FINAL QUERY
# =========================================================

async def get(self,data:GetAllStockAdjSchema):

    created_at=func.date(
        func.timezone(
            data.timezone.value,
            sa.created_at
        )
    )

    cursor=(data.offset-1)*data.limit

    select_stmt=(
        select(
            *self.stock_adj_cols,
            products_agg.label("products")
        )

        .join(
            product_subq,
            product_subq.c.stockadjustment_id == sa.id
        )

        .group_by(sa.id)

        .offset(cursor)
        .limit(data.limit)
    )

    results=(
        await self.session.execute(
            select_stmt
        )
    ).mappings().all()

    return results

class StockAdjRepo(BaseRepoModel):
    def __init__(self, session:AsyncSession):
        self.stock_adj_cols=(
            sa.id,
            sa.ui_id,
            sa.shop_id,
            sa.description,
            sa.adjusted_date,
            sa.created_at,
            sa.updated_at,
            sa.movement_type
        )

        super().__init__(session)

    @start_db_transaction
    async def create(self, data:CreateStockAdjDbSchema):
        ic(data)
        data_toadd=StockAdjustments(**data.model_dump())
        self.session.add(data_toadd)
        return True
    
    @start_db_transaction
    async def create_bulk_stockadj_inv_prod(self, datas: List[StockAdjustmentInventoryProducts]):
        ic(datas)
        self.session.add_all(datas)
        return True
    
    @start_db_transaction
    async def create_bulk(self,datas:List[StockAdjustments]):
        res=self.session.add_all(datas)
        return True
    
    
    @start_db_transaction
    async def update(self,data:CreateStockAdjDbSchema):
        stock_adj_data=data.model_dump(mode="json",exclude_unset=True)
        
        ic("inside nn")
        stock_adj_toupdate=(
                update(StockAdjustments)
                .where(
                    StockAdjustments.id==data.id,
                    StockAdjustments.shop_id==data.shop_id
                )
                .values(datas=data.datas)
        ).returning(StockAdjustments.id)
        is_updated=(await self.session.execute(stock_adj_toupdate)).scalar_one_or_none()

        if not is_updated:
            return False

        return True
        
    
    @start_db_transaction
    async def delete(self,stock_adj_id:str,shop_id:str):
        stock_adj_del=(
            delete(StockAdjustments)
            .where(
                StockAdjustments.id==stock_adj_id,
                StockAdjustments.shop_id==shop_id
            )
        ).returning(StockAdjustments.id)

        is_deleted=(await self.session.execute(stock_adj_del)).scalar_one_or_none()

        return is_deleted
    
    async def bulk_check(self,shop_id:str,stock_adj_ids:list):
        check_stmt=(
            select(
                StockAdjustments.id
            )
            .where(
                StockAdjustments.id.in_(stock_adj_ids),
                StockAdjustments.shop_id==shop_id
            )
        )

        results=(await self.session.execute(check_stmt)).scalars().all()

        ic(results)

        return results
        
    async def getby_shop_id(self,data:GetStockAdjByShopIdSchema):
        created_at=func.date(func.timezone(data.timezone.value,StockAdjustments.created_at))
        cursor=(data.offset-1)*data.limit
        ic(data.shop_id,data.query)
        select_stmt=(
            select(
                *self.stock_adj_cols,
                products_agg.label("products")
            )
            .join(sap, sap.stockadjustment_id == sa.id)
            .join(i, i.id == sap.inventory_id)
            .outerjoin(v, v.id == sap.variant_id)
            .outerjoin(b, b.id == sap.batch_id)
            .where(
                StockAdjustments.shop_id==data.shop_id
            )
            .group_by(sa.id)
            .offset(offset=cursor).limit(limit=data.limit)
        )

        results=(
            await self.session.execute(
                select_stmt
            )
        ).mappings().all()

        return results
    
    async def getby_inventory_id(self,data:GetStockAdjByInventoryIdSchema):
        select_stmt=(
            select(
                *self.stock_adj_cols,
                products_agg.label("products")
            )
            .join(
                sap_agg,
                sap_agg.c.stockadjustment_id == sa.id
            )
            .join(
                i,
                i.id == sap_agg.c.inventory_id
            )
            .outerjoin(
                variant_subq,
                and_(
                    variant_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    variant_subq.c.inventory_id ==
                    sap_agg.c.inventory_id,

                    variant_subq.c.variant_id ==
                    sap_agg.c.variant_id
                )
            )
            .outerjoin(
                batch_subq,
                and_(
                    batch_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    batch_subq.c.inventory_id ==
                    sap_agg.c.inventory_id
                )
            )
            .outerjoin(
                direct_serial_subq,
                and_(
                    direct_serial_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    direct_serial_subq.c.inventory_id ==
                    sap_agg.c.inventory_id
                )
            )
            .group_by(sa.id)
            .where(
                StockAdjustments.shop_id==data.shop_id,
                StockAdjustmentInventoryProducts.inventory_id==data.inventory_id
            )
        )

        results=(
            await self.session.execute(
                select_stmt
            )
        ).mappings().all()

        return results
    
    

    async def get(self,data:GetAllStockAdjSchema):
        created_at=func.date(func.timezone(data.timezone.value,StockAdjustments.created_at))
        cursor=(data.offset-1)*data.limit
        select_stmt=(
            select(
                *self.stock_adj_cols,
                products_agg.label("products")
            )
            .join(
                sap_agg,
                sap_agg.c.stockadjustment_id == sa.id
            )
            .join(
                i,
                i.id == sap_agg.c.inventory_id
            )
            .outerjoin(
                variant_subq,
                and_(
                    variant_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    variant_subq.c.inventory_id ==
                    sap_agg.c.inventory_id,

                    variant_subq.c.variant_id ==
                    sap_agg.c.variant_id
                )
            )
            .outerjoin(
                batch_subq,
                and_(
                    batch_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    batch_subq.c.inventory_id ==
                    sap_agg.c.inventory_id
                )
            )
            .outerjoin(
                direct_serial_subq,
                and_(
                    direct_serial_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    direct_serial_subq.c.inventory_id ==
                    sap_agg.c.inventory_id
                )
            )
            .group_by(sa.id)
            .offset(cursor)
            .limit(data.limit)
        )

        results=(
            await self.session.execute(
                select_stmt
            )
        ).mappings().all()

        return results
    
    async def getby_id(self,data:GetStockAdjByIdSchema):
        created_at=func.date(func.timezone(data.timezone.value,StockAdjustments.created_at))
        select_stmt=(
            select(
                *self.stock_adj_cols,
                products_agg.label("products")
            )
            .join(
                sap_agg,
                sap_agg.c.stockadjustment_id == sa.id
            )
            .join(
                i,
                i.id == sap_agg.c.inventory_id
            )
            .outerjoin(
                variant_subq,
                and_(
                    variant_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    variant_subq.c.inventory_id ==
                    sap_agg.c.inventory_id,

                    variant_subq.c.variant_id ==
                    sap_agg.c.variant_id
                )
            )
            .outerjoin(
                batch_subq,
                and_(
                    batch_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    batch_subq.c.inventory_id ==
                    sap_agg.c.inventory_id
                )
            )
            .outerjoin(
                direct_serial_subq,
                and_(
                    direct_serial_subq.c.stockadjustment_id ==
                    sap_agg.c.stockadjustment_id,

                    direct_serial_subq.c.inventory_id ==
                    sap_agg.c.inventory_id
                )
            )
            .group_by(sa.id)
            .where(
                StockAdjustments.shop_id==data.shop_id,
                StockAdjustments.id==data.id
            )
        )

        results=(await self.session.execute(select_stmt)).mappings().one_or_none()

        return results

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """
        


        