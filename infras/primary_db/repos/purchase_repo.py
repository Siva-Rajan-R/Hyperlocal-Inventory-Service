from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case,literal,literal_column
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.purchase_model import Purchase,PurchaseInventoryProducts
from ..models.inventory_model import Inventory,InventoryBatches,InventorySerialNumbers,InventoryVariants
from schemas.v1.db_schemas.purchase_schema import CreatePurchaseDbSchema,UpdatePurchaseDbSchema
from schemas.v1.request_schemas.purchase_schema import BulkCheckPurchaseSchema,GetPurchaseByShopIdSchema,GetPurchaseByIdSchema,GetPurchaseByInventoryIdSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums,PurchaseViewsEnums

# aliases (optional but cleaner)
p = Purchase
pip = PurchaseInventoryProducts
i = Inventory
v = InventoryVariants
b = InventoryBatches
s = InventorySerialNumbers

pip_agg = (
    select(
        pip.purchase_id,
        pip.inventory_id,
        pip.variant_id,
        pip.batch_id,
        func.sum(pip.stocks).label("stocks"),
        func.max(pip.buy_price).label("buy_price"),
        func.max(pip.sell_price).label("sell_price"),
    )
    .group_by(
        pip.purchase_id,
        pip.inventory_id,
        pip.variant_id,
        pip.batch_id
    )
).subquery()

serial_subq = (
    select(
        func.jsonb_build_object(
            "id", s.id,
            "serial_numbers", s.serial_numbers
        )
    )
    .where(s.batch_id == b.id)
    .limit(1)
    .scalar_subquery()
)

batch_subq = (
    select(
        pip_agg.c.purchase_id,        # ✅ ADD THIS
        pip_agg.c.variant_id,
        func.jsonb_agg(
            func.distinct(           # ✅ IMPORTANT
                func.jsonb_build_object(
                    "id", b.id,
                    "name", b.name,
                    "stocks", pip_agg.c.stocks,
                    "expiry_date", b.expiry_date,
                    "manufacturing_date", b.manufacturing_date
                )
            )
        ).label("batches")
    )
    .join(b, b.id == pip_agg.c.batch_id)
    .group_by(
        pip_agg.c.purchase_id,       # ✅ ADD THIS
        pip_agg.c.variant_id
    )
).subquery()

variant_subq = (
    select(
        pip_agg.c.purchase_id,       # ✅ ADD THIS
        pip_agg.c.inventory_id,
        func.jsonb_agg(
            func.distinct(           # ✅ IMPORTANT
                func.jsonb_build_object(
                    "id", v.id,
                    "name", v.name,
                    "stocks", pip_agg.c.stocks,
                    "buy_price", pip_agg.c.buy_price,
                    "sell_price", pip_agg.c.sell_price,
                    "batches", batch_subq.c.batches
                )
            )
        ).label("variants")
    )
    .join(v, v.id == pip_agg.c.variant_id)
    .outerjoin(
        batch_subq,
        and_(
            batch_subq.c.variant_id == v.id,
            batch_subq.c.purchase_id == pip_agg.c.purchase_id   # ✅ CRITICAL
        )
    )
    .group_by(
        pip_agg.c.purchase_id,       # ✅ ADD THIS
        pip_agg.c.inventory_id
    )
).subquery()


# product_subq = (
#     select(
#         pip_agg.c.purchase_id,
#         pip_agg.c.inventory_id,

#         func.sum(pip_agg.c.stocks).label("stocks"),
#         func.max(pip_agg.c.buy_price).label("buy_price"),
#         func.max(pip_agg.c.sell_price).label("sell_price"),

#         variant_subq.c.variants
#     )
#     .outerjoin(
#         variant_subq,
#         and_(
#             variant_subq.c.inventory_id == pip_agg.c.inventory_id,
#             variant_subq.c.purchase_id == pip_agg.c.purchase_id
#         )
#     )
#     .group_by(
#         pip_agg.c.purchase_id,
#         pip_agg.c.inventory_id,
#         variant_subq.c.variants
#     )
# ).subquery()


product_subq = (
    select(
        pip_agg.c.purchase_id,
        pip_agg.c.inventory_id,

        # 🔹 inventory fields
        i.id.label("id"),
        i.ui_id.label("ui_id"),
        i.sequence_id.label("sequence_id"),
        i.barcode.label("barcode"),
        i.shop_id.label("shop_id"),
        i.added_by.label("added_by"),
        i.name.label("name"),
        i.description.label("description"),
        i.category.label("category"),
        i.created_at.label("created_at"),
        i.updated_at.label("updated_at"),
        i.has_batch.label("has_batch"),
        i.has_serialno.label("has_serialno"),
        i.has_variant.label("has_variant"),

        # 🔹 aggregated purchase values
        func.sum(pip_agg.c.stocks).label("stocks"),
        func.max(pip_agg.c.buy_price).label("buy_price"),
        func.max(pip_agg.c.sell_price).label("sell_price"),

        # 🔹 nested variants
        variant_subq.c.variants
    )
    .join(i, i.id == pip_agg.c.inventory_id)  # ✅ move join here
    .outerjoin(
        variant_subq,
        and_(
            variant_subq.c.inventory_id == pip_agg.c.inventory_id,
            variant_subq.c.purchase_id == pip_agg.c.purchase_id
        )
    )
    .group_by(
        pip_agg.c.purchase_id,
        pip_agg.c.inventory_id,

        # 🔴 ALL non-aggregated columns MUST be grouped
        i.id,
        i.ui_id,
        i.sequence_id,
        i.barcode,
        i.shop_id,
        i.added_by,
        i.name,
        i.description,
        i.category,
        i.created_at,
        i.updated_at,
        i.has_batch,
        i.has_serialno,
        i.has_variant,

        variant_subq.c.variants
    )
).subquery()


products_agg = func.jsonb_agg(
    func.jsonb_build_object(
        "id", product_subq.c.id,
        "ui_id", product_subq.c.ui_id,
        "sequence_id", product_subq.c.sequence_id,
        "barcode", product_subq.c.barcode,
        "shop_id", product_subq.c.shop_id,
        "added_by", product_subq.c.added_by,
        "name", product_subq.c.name,
        "description", product_subq.c.description,
        "category", product_subq.c.category,
        "created_at", product_subq.c.created_at,
        "updated_at", product_subq.c.updated_at,
        "has_batch", product_subq.c.has_batch,
        "has_serialno", product_subq.c.has_serialno,
        "has_variant", product_subq.c.has_variant,

        # 🔹 purchase values
        "stocks", product_subq.c.stocks,
        "buy_price", product_subq.c.buy_price,
        "sell_price", product_subq.c.sell_price,

        # 🔹 nested
        "variants", product_subq.c.variants
    )
)

# products_agg = func.json_agg(
#     func.jsonb_build_object(
#         "id", i.id,
#         "name", i.name,
#         "stocks", product_subq.c.stocks,
#         "buy_price", product_subq.c.buy_price,
#         "sell_price", product_subq.c.sell_price,
#         "variants", product_subq.c.variants
#     )
# )

class PurchaseRepo(BaseRepoModel):
    def __init__(self, session:AsyncSession):
        self.session=session
        self.purchase_cols=(
            Purchase.id,
            Purchase.shop_id,
            Purchase.datas,
            Purchase.type,
            Purchase.purchase_view,
            Purchase.sequence_id,
            Purchase.ui_id,
            Purchase.supplier_id,
            Purchase.calculations,
            Purchase.additional_charges,
            Purchase.created_at,
            Purchase.updated_at
        )

    @start_db_transaction
    async def create(self,data:CreatePurchaseDbSchema):
        data_toadd=Purchase(**data.model_dump(mode='json'))
        self.session.add(data_toadd)
        return True
    
    @start_db_transaction
    async def create_purchase_inv_bulk(self,data:List[PurchaseInventoryProducts])-> bool:
        self.session.add_all(data)
        return True
    
    @start_db_transaction
    async def update(self,data:UpdatePurchaseDbSchema):
        data_toupdate=update(
            Purchase
        ).where(
            Purchase.id==data.id,
            Purchase.shop_id==data.shop_id
        ).values(
            **data.model_dump(mode="json",exclude=['id','shop_id'],exclude_none=True,exclude_unset=True)
        ).returning(Purchase.id)

        is_updated=(await self.session.execute(data_toupdate)).scalar_one_or_none()

        return is_updated
    
    @start_db_transaction
    async def delete(self,id:str,shop_id:str):
        data_todel=delete(
            Purchase
        ).where(Purchase.id==id,Purchase.shop_id==shop_id).returning(Purchase.id)

        is_deleted=(await self.session.execute(data_todel)).scalar_one_or_none()

        return is_deleted

    

    async def get(self,data:GetPurchaseByShopIdSchema):
        created_at=func.date(func.timezone(data.timezone.value,Purchase.created_at))
        cursor=(data.offset-1)*data.limit
        view_mapper={
            PurchaseViewsEnums.PURCHASE_VIEW.value:(Purchase.purchase_view==True),
            PurchaseViewsEnums.STOCKADJUSTMENT_VIEW.value:(Purchase.purchase_view==True),
            PurchaseViewsEnums.PO_VIEW.value:(and_(Purchase.purchase_view==False,Purchase.type==PurchaseTypeEnums.PO_CREATE.value))
        }

        query_stmt = (
            select(
                *self.purchase_cols,
                products_agg.label("products")
            )
            .join(product_subq, product_subq.c.purchase_id == p.id)   # ✅ use subquery
            .join(i, i.id == product_subq.c.inventory_id)
            .where(
                p.shop_id == data.shop_id,
                view_mapper[data.view.value]
            )
            .group_by(p.id)
        )

        results=(
            await self.session.execute(
                query_stmt
            )
        ).mappings().all()

        return results
    

    async def getby_id(self,data:GetPurchaseByIdSchema):
        query_stmt = (
            select(
                *self.purchase_cols,
                products_agg.label("products")
            )
            .join(product_subq, product_subq.c.purchase_id == p.id)   # ✅ use subquery
            .join(i, i.id == product_subq.c.inventory_id)
            .where(
                Purchase.id==data.id,
                Purchase.shop_id==data.shop_id
            )
            .group_by(p.id)
        )

        result=(await self.session.execute(query_stmt)).mappings().one_or_none()

        return result
    
    async def getby_inventory_id(self,data:GetPurchaseByInventoryIdSchema):
        query_stmt = (
            select(
                *self.purchase_cols,
                products_agg.label("products")
            )
            .join(product_subq, product_subq.c.purchase_id == p.id)   # ✅ use subquery
            .join(i, i.id == product_subq.c.inventory_id)
            .where(
                PurchaseInventoryProducts.inventory_id==data.inventory_id,
                Purchase.shop_id==data.shop_id
            )
            .group_by(p.id)
        )

        result=(await self.session.execute(query_stmt)).mappings().all()

        return result

    async def search(self, query, limit = 5):
        ...
        

    async def bulk_check_purchase_inv_products(self,data:BulkCheckPurchaseSchema):
        ic(data.purchase_id,data.inventory_id)
        check_stmt=(
            select(
                PurchaseInventoryProducts.id
            )
            .where(
                PurchaseInventoryProducts.inventory_id.in_(data.inventory_id),
                PurchaseInventoryProducts.purchase_id==data.purchase_id
            )
        )

        results=(await self.session.execute(check_stmt)).mappings().all()

        ic(results)

        return results
