from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select,update,delete,func,or_,and_,String,case,cast,Text
from sqlalchemy.dialects.postgresql import JSONB,ARRAY
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.inventory_model import Inventory,InventoryVariants,InventoryBatches,InventorySerialNumbers
from schemas.v1.db_schemas.inventory_schema import InventoryProductCategoryEnum,CreateInventoryDbSchema,UpdateInventoryDbSchema,UpdateVarientProductDbSchema,InventoryBatchDbSchema
from schemas.v1.request_schemas.inventory_schema import DeleteInventorySchema,GetAllInventorySchema,GetInventoryByIdSchema,GetInventoryByShopIdSchema,VerifySchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum

class InventoryRepo(BaseRepoModel):
    def __init__(self, session:AsyncSession):
        self.inv_cols=(
            Inventory.id,
            Inventory.ui_id,
            Inventory.sequence_id,
            Inventory.barcode,
            Inventory.shop_id,
            Inventory.added_by,
            Inventory.buy_price,
            Inventory.sell_price,
            Inventory.stocks,
            Inventory.datas,
            Inventory.name,
            Inventory.description,
            Inventory.category,
            Inventory.created_at,
            Inventory.updated_at,
            Inventory.has_batch,
            Inventory.has_serialno,
            Inventory.has_variant
        )

        super().__init__(session)

    @start_db_transaction
    async def create(self,data:CreateInventoryDbSchema)-> dict | None:
        filtered_data=data.model_dump(mode="json",exclude=['offer_offline','offer_online','offer_type'])
        ic(filtered_data)
        stmt=(
            insert(
                Inventory
            )
            .values(
                **filtered_data
            )
            .returning(*self.inv_cols)
        )

        res=(await self.session.execute(stmt)).mappings().one_or_none()
        ic(res)
        return data
    
    @start_db_transaction
    async def create_bulk(self,datas:List[Inventory])-> bool:
        self.session.add_all(datas)
        return True
    
    @start_db_transaction
    async def create_variant_bulk(self,datas:List[InventoryVariants])-> bool:
        self.session.add_all(datas)
        return True
    
    @start_db_transaction
    async def create_batch_bulk(self,datas:List[InventoryBatches])-> bool:
        self.session.add_all(datas)
        return True
    
    @start_db_transaction
    async def create_serialno_bulk(self,datas:List[InventorySerialNumbers])-> bool:
        self.session.add_all(datas)
        return True
    

    
    @start_db_transaction
    async def update(self,data:UpdateInventoryDbSchema)-> dict | NotImplementedError:
        inven_data=data.model_dump(mode="json",exclude=['id','shop_id','barcode','offer_offline','offer_online','offer_type'],exclude_unset=True,exclude_none=True)
        
        if inven_data or len(inven_data)>0:
            ic("inside nn",inven_data)
            inve_toupdate=(
                update(Inventory)
                .where(
                    Inventory.id==data.id,
                    Inventory.shop_id==data.shop_id
                )
                .values(**inven_data)
            ).returning(*self.inv_cols)
            inven_updated=(await self.session.execute(inve_toupdate)).mappings().one_or_none()
            ic(inven_updated)

            return inven_updated
        
        return None
    
    
    @start_db_transaction
    async def update_qty(self,barcode_inv_id:str,shop_id:str,qty:int):
        inv_qty_toupdate=update(
            Inventory
        ).where(
            or_(
                Inventory.id==barcode_inv_id,
                Inventory.barcode==barcode_inv_id
            ),
            Inventory.shop_id==shop_id
        ).values(
            stocks=qty
        ).returning(Inventory.id)

        return (await self.session.execute(inv_qty_toupdate)).scalar_one_or_none()
        
    
    @start_db_transaction
    async def delete(self,data:DeleteInventorySchema):
        invto_del=(
            delete(Inventory)
            .where(
                Inventory.id==data.id,
                Inventory.shop_id==data.shop_id
            )
        ).returning(*self.inv_cols)

        is_deleted=(await self.session.execute(invto_del)).mappings().one_or_none()

        return is_deleted
    
    @start_db_transaction
    async def bulk_qty_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        inv_qty_toupdate=update(
            Inventory
        ).where(
            Inventory.barcode.in_(data.keys()),
            Inventory.shop_id==shop_id
        ).values(
            stocks=Inventory.stocks + case(
                data,
                value=Inventory.barcode
            )
        ).returning(Inventory.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic(is_updated)
        return is_updated
    

    @start_db_transaction
    async def bulk_variant_qty_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        inv_qty_toupdate=update(
            InventoryVariants
        ).where(
            InventoryVariants.id.in_(data.keys()),
            InventoryVariants.shop_id==shop_id
        ).values(
            stocks=InventoryVariants.stocks + case(
                data,
                value=InventoryVariants.id
            )
        ).returning(InventoryVariants.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic(is_updated)
        return is_updated
    
    @start_db_transaction
    async def bulk_variant_decr_qty_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        inv_qty_toupdate=update(
            InventoryVariants
        ).where(
            InventoryVariants.id.in_(data.keys()),
            InventoryVariants.shop_id==shop_id
        ).values(
            stocks=InventoryVariants.stocks - case(
                data,
                value=InventoryVariants.id
            )
        ).returning(InventoryVariants.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic(is_updated)
        return is_updated
    
    @start_db_transaction
    async def bulk_batch_qty_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        inv_qty_toupdate=update(
            InventoryBatches
        ).where(
            or_(
                InventoryBatches.id.in_(data.keys()),
                InventoryBatches.name.in_(data.keys())
            ),
            InventoryBatches.shop_id==shop_id
        ).values(
            stocks=InventoryBatches.stocks + case(
                data,
                value=InventoryBatches.id
            )
        ).returning(InventoryBatches.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic(is_updated)
        return is_updated
    
    @start_db_transaction
    async def bulk_batch_decr_qty_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        ic(data)
        if not data:
            return True
        
        inv_qty_toupdate=update(
            InventoryBatches
        ).where(
            or_(
                InventoryBatches.id.in_(data.keys()),
                InventoryBatches.name.in_(data.keys())
            ),
            InventoryBatches.shop_id==shop_id
        ).values(
            stocks=InventoryBatches.stocks - case(
                data,
                value=InventoryBatches.id
            )
        ).returning(InventoryBatches.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic(is_updated)
        return is_updated
    
    
    @start_db_transaction
    async def bulk_inventory_batch_qty_add_update(self,datas:List[InventoryBatchDbSchema]):
        for data in datas:
            inv_qty_toupdate = (
                update(InventoryBatches)
                .where(
                    or_(
                        InventoryBatches.id == data.id,
                        InventoryBatches.name == data.name
                    ),
                    InventoryBatches.shop_id == data.shop_id,
                    InventoryBatches.inventory_id == data.inventory_id,
                    (InventoryBatches.variant_id == data.variant_id)
                    if data.variant_id else True,
                )
                .values(
                    stocks=InventoryBatches.stocks + data.stocks  # ✅ FIX
                )
                .returning(InventoryBatches.id)
            )

            is_updated = (await self.session.execute(inv_qty_toupdate)).scalars().all()

            if not is_updated:
                self.session.add(
                    InventoryBatches(**data.model_dump())
                )
            
        return True
    

    @start_db_transaction
    async def bulk_serialnumber_update(self, data: dict, shop_id: str):
        """
        data = {
            barcode: [serial_numbers]
        }
        """
        if not data:
            return True

        update_stmt = (
            update(Inventory)
            .where(
                Inventory.barcode.in_(data.keys()),
                Inventory.shop_id == shop_id
            )
            .values(
                datas=func.jsonb_set(
                    Inventory.datas,
                    cast(['serial_numbers'], ARRAY(Text)),  # ✅ FIX
                    func.coalesce(
                        Inventory.datas['serial_numbers'],
                        cast('[]', JSONB)
                    ) + case(
                        {k: cast(v, JSONB) for k, v in data.items()},
                        value=Inventory.barcode
                    )
                )
            )
            .returning(Inventory.id)
        )

        result = await self.session.execute(update_stmt)
        return result.scalars().all()
    
    @start_db_transaction
    async def bulk_serialnumber_remove(self, data: dict, shop_id: str):

        if not data:
            return []

        results = []

        for barcode, remove_list in data.items():

            elem = func.jsonb_array_elements_text(
                func.coalesce(
                    func.coalesce(Inventory.datas, cast('{}', JSONB))['serial_numbers'],
                    cast('[]', JSONB)
                )
            ).table_valued("value").alias("elem")

            update_stmt = (
                update(Inventory)
                .where(
                    Inventory.barcode == barcode,
                    Inventory.shop_id == shop_id
                )
                .values(
                    datas=func.jsonb_set(
                        func.coalesce(Inventory.datas, cast('{}', JSONB)),  # ✅ KEY FIX
                        cast(['serial_numbers'], ARRAY(Text)),
                        func.coalesce(
                            select(
                                func.jsonb_agg(elem.c.value)
                            )
                            .select_from(elem)
                            .where(
                                ~elem.c.value.in_(remove_list)
                            )
                            .scalar_subquery(),
                            cast('[]', JSONB)
                        ),
                        True   # ✅ create key if missing
                    )
                )
                .returning(Inventory.id)
            )

            res = await self.session.execute(update_stmt)
            results.extend(res.scalars().all())

        return results
    
    @start_db_transaction
    async def bulk_variant_serialnumber_update(self, data: dict, shop_id: str):
        """
        data = {
            variant_id: [serial_numbers]
        }
        """

        if not data:
            return True

        update_stmt = (
            update(InventoryVariants)
            .where(
                InventoryVariants.id.in_(data.keys()),
                InventoryVariants.shop_id == shop_id
            )
            .values(
                datas=func.jsonb_set(
                    InventoryVariants.datas,
                    cast(['serial_numbers'], ARRAY(Text)),  # ✅ FIX
                    func.coalesce(
                        InventoryVariants.datas['serial_numbers'],
                        cast('[]', JSONB)
                    ) + case(
                        {k: cast(v, JSONB) for k, v in data.items()},
                        value=InventoryVariants.id
                    )
                )
            )
            .returning(InventoryVariants.id)
        )

        result = await self.session.execute(update_stmt)
        return result.scalars().all()
    
    @start_db_transaction
    async def bulk_variant_serialnumber_remove(self, data: dict, shop_id: str):

        if not data:
            return []

        remove_case = case(
            *[(InventoryVariants.id == k, cast(v, JSONB)) for k, v in data.items()],
            else_=cast([], JSONB)
        )

        # ✅ TEXT-based extraction (key fix)
        elem = func.jsonb_array_elements_text(
            func.coalesce(
                InventoryVariants.datas['serial_numbers'],
                cast([], JSONB)
            )
        ).table_valued("value").alias("elem")

        update_stmt = (
            update(InventoryVariants)
            .where(
                InventoryVariants.id.in_(list(data.keys())),
                InventoryVariants.shop_id == shop_id
            )
            .values(
                datas=func.jsonb_set(
                    InventoryVariants.datas,
                    cast(['serial_numbers'], ARRAY(Text)),
                    func.coalesce(
                        select(
                            func.jsonb_agg(elem.c.value)  # text → jsonb auto
                        )
                        .select_from(elem)
                        .where(
                            ~elem.c.value.in_(
                                select(
                                    func.jsonb_array_elements_text(remove_case)
                                )
                            )
                        )
                        .scalar_subquery(),
                        cast('[]', JSONB)
                    )
                )
            )
            .returning(InventoryVariants.id)
        )

        result = await self.session.execute(update_stmt)
        return result.scalars().all()
    

    @start_db_transaction
    async def bulk_variant_update(self,datas:List[UpdateVarientProductDbSchema]):
        result=[]
        for update_data in datas:
            structured_data=update_data.model_dump(mode='json',exclude=["shop_id","id","inventory_id","barcode","stocks"],exclude_none=True,exclude_unset=True)
            is_updated=(await self.session.execute(
                update(
                    InventoryVariants
                ).where(
                    InventoryVariants.id==update_data.id,
                    InventoryVariants.shop_id==update_data.shop_id,
                    InventoryVariants.inventory_id==update_data.inventory_id
                ).values(
                    **structured_data
                ).returning(InventoryVariants.id)
            )).scalar_one_or_none()

            if not is_updated:
                self.session.add(
                    InventoryVariants(
                        **update_data.model_dump()
                    )
                )
                ic("Variant Added:",update_data.id)
                result.append(update_data.id)
            if is_updated:
                ic("Variant Updated:",update_data.id)
                result.append(is_updated)

        ic("Bulk Variant Update Result:",result)
        if len(result)==len(datas):
            return True
        ic("Failed to update all variants")
        return False

    @start_db_transaction
    async def bulk_sellprice_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        ic(data)
        inv_qty_toupdate=update(
            Inventory
        ).where(
            Inventory.barcode.in_(data.keys()),
            Inventory.shop_id==shop_id
        ).values(
            sell_price=case(
                data,
                value=Inventory.barcode
            )
        ).returning(Inventory.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic("Sell Price Updated:",is_updated)
        return is_updated
    

    @start_db_transaction
    async def bulk_buyprice_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        ic(data)
        inv_qty_toupdate=update(
            Inventory
        ).where(
            Inventory.barcode.in_(data.keys()),
            Inventory.shop_id==shop_id
        ).values(
            buy_price=case(
                data,
                value=Inventory.barcode
            )
        ).returning(Inventory.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic("Buy Price Updated:",is_updated)
        ic(is_updated)
        return is_updated
    

    @start_db_transaction
    async def bulk_variant_sellprice_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        inv_qty_toupdate=update(
            InventoryVariants
        ).where(
            InventoryVariants.id.in_(data.keys()),
            InventoryVariants.shop_id==shop_id
        ).values(
            sell_price=case(
                data,
                value=InventoryVariants.id
            )
        ).returning(InventoryVariants.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic("Variant Sell Price Updated:",is_updated)
        return is_updated
    

    @start_db_transaction
    async def bulk_variant_buyprice_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        inv_qty_toupdate=update(
            InventoryVariants
        ).where(
            InventoryVariants.id.in_(data.keys()),
            InventoryVariants.shop_id==shop_id
        ).values(
            buy_price=case(
                data,
                value=InventoryVariants.id
            )
        ).returning(InventoryVariants.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic("Variant Buy Price Updated:",is_updated)
        return is_updated
    
    @start_db_transaction
    async def bulk_qty_decr_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
        if not data:
            return True
        inv_qty_toupdate=update(
            Inventory
        ).where(
            Inventory.barcode.in_(data.keys()),
            Inventory.shop_id==shop_id
        ).values(
            stocks=Inventory.stocks - case(
                data,
                value=Inventory.barcode
            )
        ).returning(Inventory.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic("Quantity Updated:",is_updated)
        return is_updated
    
    async def bulk_check(self,shop_id:str,barcodes:list,additional_conditions: Optional[tuple]=()):
        check_stmt=(
            select(
                Inventory.id,
                Inventory.barcode,
                Inventory.datas
            )
            .where(
                Inventory.barcode.in_(barcodes),
                Inventory.shop_id==shop_id,
                *additional_conditions
            )
        )

        results=(await self.session.execute(check_stmt)).mappings().all()

        ic(results)

        return results
    

    async def bulk_varient_check(self,shop_id:str,variants_id:list,additional_conditions: Optional[tuple]=()):
        check_stmt=(
            select(
                InventoryVariants.id,
                InventoryVariants.inventory_id,
                InventoryVariants.datas
            )
            .where(
                InventoryVariants.id.in_(variants_id),
                InventoryVariants.shop_id==shop_id,
                *additional_conditions
            )
        )

        results=(await self.session.execute(check_stmt)).mappings().all()

        ic(results)

        return results
        
    async def getby_shop_id(self,data:GetInventoryByShopIdSchema)-> List[dict] | list:
        created_at=func.date(func.timezone(data.timezone.value,Inventory.created_at))
        cursor=(data.offset-1)*data.limit
        select_stmt=(
            select(*self.inv_cols)
            .where(
                Inventory.shop_id==data.shop_id,
                or_(
                    Inventory.id.ilike(data.query),
                    Inventory.barcode.ilike(data.query),
                    Inventory.shop_id.ilike(data.query),
                    Inventory.added_by.ilike(data.query),
                    func.cast(created_at,String).ilike(data.query)
                )
            )
            .offset(offset=cursor).limit(limit=data.limit)
        )
        results=(
            await self.session.execute(
                select_stmt 
            )
        ).mappings().all()

        return results
    
    async def get(self,data:GetAllInventorySchema)-> List[dict] | list:
        created_at=func.date(func.timezone(data.timezone.value,Inventory.created_at))
        cursor=(data.offset-1)*data.limit
        variants_json = case(
    (
        Inventory.has_variant == True,
        func.coalesce(
            func.jsonb_agg(
                func.distinct(
                    func.jsonb_build_object(
                        "id", InventoryVariants.id,
                        "name", InventoryVariants.name,
                        "sell_price", InventoryVariants.sell_price,
                        "buy_price", InventoryVariants.buy_price,
                        "stocks", InventoryVariants.stocks,
                        "datas", InventoryVariants.datas,
                        "batches",
                        select(
                            func.coalesce(
                                func.jsonb_agg(
                                    func.jsonb_build_object(
                                        "id", InventoryBatches.id,
                                        "name", InventoryBatches.name,
                                        "expiry_date", InventoryBatches.expiry_date,
                                        "manufacturing_date", InventoryBatches.manufacturing_date,
                                        "stocks", InventoryBatches.stocks,
                                        "serial_numbers",
                                        select(
                                            func.coalesce(
                                                func.jsonb_agg(InventorySerialNumbers.serial_numbers),
                                                func.cast("[]", JSONB)
                                            )
                                        )
                                        .where(InventorySerialNumbers.batch_id == InventoryBatches.id)
                                        .correlate(InventoryBatches)
                                        .scalar_subquery()
                                    )
                                ),
                                func.cast("[]", JSONB)
                            )
                        )
                        .where(InventoryBatches.variant_id == InventoryVariants.id)
                        .correlate(InventoryVariants)
                        .scalar_subquery()
                    )
                )
            ).filter(InventoryVariants.id.isnot(None)),
            func.cast("[]", JSONB)
        )
    ),
    else_=
        # 🔥 NO VARIANT CASE
        func.coalesce(
            func.jsonb_agg(
                func.distinct(
                    func.jsonb_build_object(
                        "id", Inventory.id,
                        "name", Inventory.name,

                        "batches",
                        select(
                            func.coalesce(
                                func.jsonb_agg(
                                    func.jsonb_build_object(
                                        "id", InventoryBatches.id,
                                        "name", InventoryBatches.name,
                                        "expiry_date", InventoryBatches.expiry_date,
                                        "manufacturing_date", InventoryBatches.manufacturing_date,
                                        "stocks", InventoryBatches.stocks,
                                        "serial_numbers",
                                        select(
                                            func.coalesce(
                                                func.jsonb_agg(InventorySerialNumbers.serial_numbers),
                                                func.cast("[]", JSONB)
                                            )
                                        )
                                        .where(InventorySerialNumbers.batch_id == InventoryBatches.id)
                                        .correlate(InventoryBatches)
                                        .scalar_subquery()
                                    )
                                ),
                                func.cast("[]", JSONB)
                            )
                        )
                        .where(InventoryBatches.inventory_id == Inventory.id)
                        .correlate(Inventory)
                        .scalar_subquery()
                    )
                )
            ),
            func.cast("[]", JSONB)
        )
).label("variants")
        select_stmt=(
            select(
                *self.inv_cols,
                variants_json
            )
            .outerjoin(
        InventoryVariants,
        InventoryVariants.inventory_id == Inventory.id
    )
    .group_by(Inventory.id)
            .where(
                or_(
                    Inventory.id.ilike(data.query),
                    Inventory.barcode.ilike(data.query),
                    Inventory.shop_id.ilike(data.query),
                    Inventory.added_by.ilike(data.query),
                    func.cast(created_at,String).ilike(data.query)
                )
            )
            .offset(offset=cursor).limit(limit=data.limit)
        )
        results=(
            await self.session.execute(
                select_stmt 
            )
        ).mappings().all()

        return results
    
    async def getby_id(self,data:GetInventoryByIdSchema)-> dict | None:
        stmt=(
            select(
                *self.inv_cols
            )
            .where(
                or_(Inventory.id==data.id,
                Inventory.barcode==data.barcode),
                Inventory.shop_id==data.shop_id
            )
        )

        res=(await self.session.execute(stmt)).mappings().one_or_none()

        return res


    async def verify(self,data:VerifySchema):
        stmt=(
            select(
                Inventory.id
            )
            .where(
                or_(Inventory.id==data.id,
                Inventory.shop_id==data.shop_id),
                Inventory.barcode==data.barcode
            )
        )

        result=(await self.session.execute(stmt)).scalar_one_or_none()

        if result:
            return {'id':result,'exists':True}
        
        return {'id':'','exists':False}

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """
        


        