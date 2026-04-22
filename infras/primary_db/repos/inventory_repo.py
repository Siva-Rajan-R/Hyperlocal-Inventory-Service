from models.repo_models.base_repo_model import BaseRepoModel
from models.service_models.base_service_model import BaseServiceModel
from sqlalchemy import select,update,delete,func,or_,and_,String,case
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.inventory_model import Inventory
from schemas.v1.db_schemas.inventory_schema import InventoryProductCategoryEnum,AddInventoryDbSchema,UpdateInventoryDbSchema
from hyperlocal_platform.core.decorators.db_session_handler_dec import start_db_transaction
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional,List
from icecream import ic
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum

class InventoryRepo(BaseRepoModel):
    def __init__(self, session:AsyncSession):
        self.inv_cols=(
            Inventory.id,
            Inventory.barcode,
            Inventory.shop_id,
            Inventory.added_by,
            Inventory.buy_price,
            Inventory.sell_price,
            Inventory.stocks,
            Inventory.datas
        )

        super().__init__(session)

    @start_db_transaction
    async def create(self, data:AddInventoryDbSchema):
        ic(data)
        datas_toadd=[
            Inventory(**data.model_dump(mode="json",exclude_unset=True,exclude_none=True,exclude=['offer_offline','offer_online','offer_type'])),
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
    async def bulk_sellprice_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
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
        ic(is_updated)
        return is_updated
    

    @start_db_transaction
    async def bulk_buyprice_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
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
        ic(is_updated)
        return is_updated
    
    @start_db_transaction
    async def bulk_qty_decr_update(self,data:dict,shop_id:str):
        """
        Docstring for bulk_qty_update
        THe data contains product barcode as a key & the qty to increment as a value
        """
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
        ic(is_updated)
        return is_updated
    
    async def bulk_check(self,shop_id:str,barcodes:list):
        check_stmt=(
            select(
                Inventory.barcode
            )
            .where(
                Inventory.barcode.in_(barcodes),
                Inventory.shop_id==shop_id
            )
        )

        results=(await self.session.execute(check_stmt)).scalars().all()

        ic(results)

        return results
        
    async def get(self,timezone:TimeZoneEnum,shop_id:str,query:str="",limit:Optional[int]=None,offset:Optional[int]=None,full:Optional[bool]=True):
        created_at=func.date(func.timezone(timezone.value,Inventory.created_at))
        select_stmt=(
            select(*self.inv_cols,created_at)
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
        


        