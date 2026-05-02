from pydantic import BaseModel,Field,EmailStr
from typing import Optional,List,Union
from datetime import date,datetime
from ...request_schemas.inventory_schema import InventoryVariantSchema,InventoryBatchSchema

class InventoryGetResponseSchema(BaseModel):
    id:str
    ui_id:str
    name:str
    description:str
    category:str
    sell_price:float
    buy_price:float
    stocks:int
    barcode:str
    shop_id:str
    added_by:str
    datas:Optional[dict]=None
    created_at:datetime
    updated_at:datetime
    has_variant:bool
    has_batch:bool
    has_serialno:bool

    variants:Optional[InventoryVariantSchema]=None
    batches:Optional[InventoryBatchSchema]=None
    serial_number:Optional[List]=None