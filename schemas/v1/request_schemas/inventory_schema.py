from pydantic import BaseModel
from typing import List,Optional,Dict
from datetime import date
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum

class ProductBatchesSchema(BaseModel):
    expiry_date:date
    mfg_date:date
    batch_name:str
    stocks:int

class ProductVarientsCreateSchema(BaseModel):
    barcode:str
    buy_price:float
    sell_price:float
    serial_numbers:Optional[List[str]]=[]
    stocks:int

class ProductVarientsUpdateSchema(BaseModel):
    id:Optional[str]=None
    buy_price:float
    sell_price:float
    serial_numbers:Optional[List[str]]=[]
    stocks:int
    barcode:str


class InventoryCreateMandatoryFields(BaseModel):
    shop_id:str
    barcode:str
    stocks:Optional[int]=None
    buy_price:float
    sell_price:float
    name:str
    description:str
    category:str
    has_varients:bool
    varients:Optional[List[ProductVarientsCreateSchema]]=[]
    serial_numbers:Optional[List[str]]=[]
    has_batch_tracking:bool
    has_serialno_tracking:bool


    model_config={
        "extra":"allow"
    }

class AddInventorySchema(BaseModel):
    datas:InventoryCreateMandatoryFields


class InventoryUpdateMandatoryFields(BaseModel):
    id:str
    shop_id:str
    barcode:str
    buy_price:float
    sell_price:float
    name:str
    description:str
    category:str
    has_varients:bool
    varients:Optional[List[ProductVarientsUpdateSchema]]=[]
    has_batch_tracking:bool
    has_serialno_tracking:bool
    serial_numbers:Optional[List[str]]=[]

    model_config={
        "extra":"allow"
    }

class UpdateInventorySchema(BaseModel):
    datas:InventoryUpdateMandatoryFields
    