from pydantic import BaseModel
from typing import List,Optional
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum
from datetime import date
from ..request_schemas.inventory_schema import InventoryCreateMandatoryFields,ProductVarientsUpdateSchema


class AddInventoryDbSchema(BaseModel):
    id:str
    shop_id:str
    barcode:str
    stocks:int
    buy_price:float
    sell_price:float
    datas:Optional[dict]={}
    added_by:str


class UpdateInventoryDbSchema(BaseModel):
    id:str
    barcode:str
    shop_id:str
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    datas:Optional[dict]={}

    model_config={
        'use_enum_values':True
    }


class UpdateVarientProductDbSchema(BaseModel):
    id:str
    shop_id:str
    inventory_id:str
    sell_price:float
    buy_price:float
    datas:dict
    barcode:str
    stocks:Optional[int]=None


class InventoryBatchDbSchema(BaseModel):
    id:str
    shop_id:str
    inventory_id:str
    variant_id:Optional[str]=None
    name:str
    stocks:int
    expiry_date:Optional[date]=None
    manufacturing_date:Optional[date]=None