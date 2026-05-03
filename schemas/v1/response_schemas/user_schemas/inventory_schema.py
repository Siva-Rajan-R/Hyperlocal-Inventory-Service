from pydantic import BaseModel,Field,EmailStr
from typing import Optional,List,Union
from datetime import date,datetime
from ...request_schemas.inventory_schema import InventoryResponseVariantSchema,InventoryBatchResponseSchema

# class InventoryCreateResponseSchema(BaseModel):
#     id:str
#     ui_id:int
#     name:str
#     description:str
#     category:str
#     sell_price:float
#     buy_price:float
#     stocks:int
#     barcode:str
#     shop_id:str
#     added_by:str
#     datas:Optional[dict]=None
#     created_at:datetime
#     updated_at:datetime
#     has_variant:bool
#     has_batch:bool
#     has_serialno:bool

#     variants:Optional[InventoryResponseVariantSchema]=None
#     batches:Optional[InventoryBatchSchema]=None
#     serial_number:Optional[List]=None


# class InventoryUpdateResponseSchema(BaseModel):
#     id:str
#     ui_id:int
#     name:str
#     description:str
#     category:str
#     sell_price:float
#     buy_price:float
#     stocks:int
#     barcode:str
#     shop_id:str
#     added_by:str
#     datas:Optional[dict]=None
#     created_at:datetime
#     updated_at:datetime
#     has_variant:bool
#     has_batch:bool
#     has_serialno:bool

#     variants:Optional[List[InventoryResponseVariantSchema]]=None
#     batches:Optional[List[InventoryBatchSchema]]=None
#     serial_number:Optional[List]=None


# class InventoryDeleteResponseSchema(BaseModel):
#     id:str
#     ui_id:int
#     name:str
#     description:str
#     category:str
#     sell_price:float
#     buy_price:float
#     stocks:int
#     barcode:str
#     shop_id:str
#     added_by:str
#     datas:Optional[dict]=None
#     created_at:datetime
#     updated_at:datetime
#     has_variant:bool
#     has_batch:bool
#     has_serialno:bool

#     variants:Optional[InventoryResponseVariantSchema]=None
#     batches:Optional[InventoryBatchSchema]=None
#     serial_number:Optional[List]=None


class InventoryGetResponseSchema(BaseModel):
    id:str
    ui_id:int
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

    variants:Optional[List[InventoryResponseVariantSchema]]=None
    batches:Optional[List[InventoryBatchResponseSchema]]=None
    serial_number:Optional[List]=None