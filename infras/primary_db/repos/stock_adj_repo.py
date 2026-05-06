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

sa = StockAdjustments
sap = StockAdjustmentInventoryProducts
i = Inventory
v = InventoryVariants
b = InventoryBatches
s = InventorySerialNumbers

serial_subq = (
    select(
        func.jsonb_build_object(
            "id", s.id,
            "serial_numbers", s.serial_numbers
        )
    )
    .where(s.batch_id == sap.batch_id)
    .correlate(sap)          # ✅ FIX
    .scalar_subquery()
)

batch_subq = (
    select(
        func.coalesce(
            func.jsonb_agg(
                func.jsonb_build_object(
                    "id", b.id,
                    "name", b.name,
                    "stocks", sap.stocks,
                    "expiry_date", b.expiry_date,
                    "manufacturing_date", b.manufacturing_date,
                    "serial_numbers", serial_subq
                )
            ),
            literal_column("'[]'::jsonb")
        )
    )
    .where(
        b.variant_id == v.id,
        b.id == sap.batch_id   # ✅ only relevant batches
    )
    .correlate(v, sap)
    .scalar_subquery()
)

variant_subq = (
    select(
        func.coalesce(
            func.jsonb_agg(
                func.distinct(
                    func.jsonb_build_object(
                        "id", v.id,
                        "name", v.name,
                        "batches", batch_subq
                    )
                )
            ),
            literal_column("'[]'::jsonb")
        )
    )
    .where(
        v.id == sap.variant_id,
        sap.inventory_id == i.id
    )
    .correlate(i, sap)
    .scalar_subquery()
)


products_agg = func.jsonb_agg(
    func.jsonb_build_object(
        "inventory_id", i.id,
        "name", i.name,
        "description", i.description,
        "category", i.category,
        "barcode",i.barcode,

        "stocks", sap.stocks,
        "type", sap.type,
        "stocks_before",sap.stocks_before,

        "has_variant", i.has_variant,
        "has_batch", i.has_batch,
        "has_serialno", i.has_serialno,

        "variants", variant_subq
    )
)

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
            .join(sap, sap.stockadjustment_id == sa.id)
            .join(i, i.id == sap.inventory_id)
            .outerjoin(v, v.id == sap.variant_id)
            .outerjoin(b, b.id == sap.batch_id)
            .where(
                StockAdjustments.shop_id==data.shop_id,
                StockAdjustmentInventoryProducts.inventory_id==data.inventory_id
            )
            .group_by(sa.id)
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
            .join(sap, sap.stockadjustment_id == sa.id)
            .join(i, i.id == sap.inventory_id)
            .outerjoin(v, v.id == sap.variant_id)
            .outerjoin(b, b.id == sap.batch_id)
            .group_by(sa.id)
            .offset(offset=cursor).limit(limit=data.limit)
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
            .join(sap, sap.stockadjustment_id == sa.id)
            .join(i, i.id == sap.inventory_id)
            .outerjoin(v, v.id == sap.variant_id)
            .outerjoin(b, b.id == sap.batch_id)
            .where(
                StockAdjustments.shop_id==data.shop_id,
                StockAdjustments.id==data.id
            )
            .group_by(sa.id)
        )

        results=(await self.session.execute(select_stmt)).mappings().one_or_none()

        return results

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """
        


        