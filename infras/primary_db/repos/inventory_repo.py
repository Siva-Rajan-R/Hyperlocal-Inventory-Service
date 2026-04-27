from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case,cast,Text
from sqlalchemy.dialects.postgresql import JSONB,ARRAY
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.inventory_model import Inventory,InventoryVariants,InventoryBathces
from schemas.v1.db_schemas.inventory_schema import InventoryProductCategoryEnum,AddInventoryDbSchema,UpdateInventoryDbSchema,UpdateVarientProductDbSchema,InventoryBatchDbSchema
from schemas.v1.request_schemas.inventory_schema import ProductVarientsUpdateSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum

class InventoryRepo(BaseRepoModel):
    def __init__(self, session:AsyncSession):

        self.variant_datas=(
            InventoryVariants.id.label("variant_id"),
            InventoryVariants.shop_id.label("variant_shop_id"),
            InventoryVariants.inventory_id.label("variant_inventory_id"),
            InventoryVariants.batch_id.label("variant_batch_id"),
            InventoryVariants.stocks.label("variant_stocks"),
            InventoryVariants.buy_price.label("variant_buy_price"),
            InventoryVariants.sell_price.label("variant_sell_price"),
            InventoryVariants.barcode.label("variant_barcode"),
            InventoryVariants.datas.label("variant_datas"),
            InventoryVariants.created_at.label("variant_created_at")
        )

        self.batch_datas=(
            InventoryBathces.id.label("batch_id"),
            InventoryBathces.shop_id.label("batch_shop_id"),
            InventoryBathces.inventory_id.label("batch_inventory_id"),
            InventoryBathces.variant_id.label("batch_variant_id"),
            InventoryBathces.stocks.label("batch_stocks"),
            InventoryBathces.expiry_date.label("batch_expiry_date"),
        )

        self.cols=(
    select(
        Inventory.id,
        Inventory.barcode,
        Inventory.stocks,
        Inventory.datas,

        func.coalesce(
            func.jsonb_agg(
                func.distinct(
                    func.jsonb_build_object(
                        "id", InventoryVariants.id,
                        "stocks", InventoryVariants.stocks,
                        "barcode", InventoryVariants.barcode,
                        "datas", InventoryVariants.datas,

                        "batches",
                        func.coalesce(
                            (
                                select(
                                    func.jsonb_agg(
                                        func.jsonb_build_object(
                                            "id", InventoryBathces.id,
                                            "stocks", InventoryBathces.stocks,
                                            "expiry_date", InventoryBathces.expiry_date,
                                            "mfg_date", InventoryBathces.manufacturing_date,
                                            "datas", InventoryBathces.datas,
                                            "name", InventoryBathces.name
                                        )
                                    )
                                )
                                .where(InventoryBathces.variant_id == InventoryVariants.id)
                                .correlate(InventoryVariants)
                                .scalar_subquery()
                            ),
                            cast('[]', JSONB)
                        )
                    )
                )
            ),
            cast('[]', JSONB)
        ).label("variants")
    )
    .outerjoin(InventoryVariants, InventoryVariants.inventory_id == Inventory.id)
    .group_by(Inventory.id)
)
        self.inv_cols=(
            Inventory.id,
            Inventory.barcode,
            Inventory.shop_id,
            Inventory.added_by,
            Inventory.buy_price,
            Inventory.sell_price,
            Inventory.stocks,
            Inventory.datas,
            self.variant_datas,
            self.batch_datas,
        )

        super().__init__(session)

    @start_db_transaction
    async def create(self, data:AddInventoryDbSchema):
        ic(data)
        filtered_data=data.model_dump(mode="json",exclude_unset=True,exclude_none=True,exclude=['offer_offline','offer_online','offer_type'])
        datas_toadd=[
            Inventory(**filtered_data),
        ]

        res=self.session.add_all(datas_toadd)
        ic(res)
        return data
    
    @start_db_transaction
    async def create_bulk(self,datas:List[Inventory]):
        res=self.session.add_all(datas)
        return True
    
    @start_db_transaction
    async def update(self,data:UpdateInventoryDbSchema):
        inven_data=data.model_dump(mode="json",exclude=['id','shop_id','barcode','offer_offline','offer_online','offer_type'],exclude_unset=True)
        
        if inven_data or len(inven_data)>0:
            ic("inside nn",inven_data)
            inve_toupdate=(
                update(Inventory)
                .where(
                    Inventory.id==data.id,
                    Inventory.shop_id==data.shop_id,
                    Inventory.barcode==data.barcode
                )
                .values(**inven_data)
            ).returning(Inventory.id)
            inven_updated=(await self.session.execute(inve_toupdate)).scalar_one_or_none()
            ic(inven_updated)

            if not inven_updated:
                return False

            return True
        
        return True
    
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
    async def delete(self,inventory_id:str,shop_id:str):
        invto_del=(
            delete(Inventory)
            .where(
                Inventory.id==inventory_id,
                Inventory.shop_id==shop_id
            )
        ).returning(Inventory.id)

        is_deleted=(await self.session.execute(invto_del)).scalar_one_or_none()

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
            InventoryBathces
        ).where(
            or_(
                InventoryBathces.id.in_(data.keys()),
                InventoryBathces.name.in_(data.keys())
            ),
            InventoryBathces.shop_id==shop_id
        ).values(
            stocks=InventoryBathces.stocks + case(
                data,
                value=InventoryBathces.id
            )
        ).returning(InventoryBathces.id)

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
            InventoryBathces
        ).where(
            or_(
                InventoryBathces.id.in_(data.keys()),
                InventoryBathces.name.in_(data.keys())
            ),
            InventoryBathces.shop_id==shop_id
        ).values(
            stocks=InventoryBathces.stocks - case(
                data,
                value=InventoryBathces.id
            )
        ).returning(InventoryBathces.id)

        is_updated=(await self.session.execute(inv_qty_toupdate)).scalars().all()
        ic(is_updated)
        return is_updated
    
    
    @start_db_transaction
    async def bulk_inventory_batch_qty_add_update(self,datas:List[InventoryBatchDbSchema]):
        for data in datas:
            inv_qty_toupdate = (
                update(InventoryBathces)
                .where(
                    or_(
                        InventoryBathces.id == data.id,
                        InventoryBathces.name == data.name
                    ),
                    InventoryBathces.shop_id == data.shop_id,
                    InventoryBathces.inventory_id == data.inventory_id,
                    (InventoryBathces.variant_id == data.variant_id)
                    if data.variant_id else True,
                )
                .values(
                    stocks=InventoryBathces.stocks + data.stocks  # ✅ FIX
                )
                .returning(InventoryBathces.id)
            )

            is_updated = (await self.session.execute(inv_qty_toupdate)).scalars().all()

            if not is_updated:
                self.session.add(
                    InventoryBathces(**data.model_dump())
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
        
    async def get(self,timezone:TimeZoneEnum,shop_id:str,query:str="",limit:Optional[int]=None,offset:Optional[int]=None,full:Optional[bool]=True):
        created_at=func.date(func.timezone(timezone.value,Inventory.created_at))
        select_stmt=(
            self.cols
            .where(
                Inventory.shop_id==shop_id,
                or_(
                    Inventory.id.ilike(query),
                    Inventory.barcode.ilike(query),
                    Inventory.shop_id.ilike(query),
                    Inventory.added_by.ilike(query),
                    func.cast(created_at,String).ilike(query)
                )
            )
        )

        if offset is not None and limit is None:
            raise ValueError("If offset provided means the limit also must be provided")
        
        if offset is not None and limit is not None:
            if offset<1:
                offset=1
            offset=(offset-1)*limit

            select_stmt.offset(offset=offset).limit(limit=limit)

        elif limit is not None:
            select_stmt.limit(limit=limit)

        ic("Hello")
        results=(
            await self.session.execute(
                select_stmt 
            )
        ).mappings().all()

        ic("jeeva",results)
        
        if not full and len(results)==1:
            results=results[0]

        return results
    
    async def getby_id(self):
        """
        Its just a wrapper method for the base repo model
        Instead use the get method with full = False
        """
        ...

    async def search(self, query, limit = 5):
        """
        This is just a wrapper method for the baserepo model
        Instead use the get method to get all kind of results by simply adjusting the limit
        """
        


        