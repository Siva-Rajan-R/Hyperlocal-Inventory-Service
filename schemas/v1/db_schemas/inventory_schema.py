from pydantic import BaseModel
from typing import List,Optional
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum
from datetime import date


class CreateInventoryDbSchema(BaseModel):
    id:str
    shop_id:str
    name:str
    category:str
    description:str
    buy_price:Optional[float]=0.0
    sell_price:Optional[float]=0.0
    stocks:Optional[int]=0
    barcode:Optional[str]=None
    is_active:Optional[bool]=False
    sku:str
    reorder_point:int
    has_variant:Optional[bool]=None
    has_serialno:Optional[bool]=None
    has_batch:Optional[bool]=None

    datas:Optional[dict]=None



class UpdateInventoryDbSchema(BaseModel):
    id:str
    shop_id:str
    name:Optional[str]=None
    category:Optional[str]=None
    description:Optional[str]=None
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None

    has_serialno:Optional[bool]=None
    has_batch:Optional[bool]=None

    datas:Optional[dict]=None


class UpdateVarientProductDbSchema(BaseModel):
    id:str
    shop_id:str
    inventory_id:str
    sell_price:float
    buy_price:float
    datas:dict
    barcode:str
    stocks:Optional[int]=None

    


# class InventoryBatchDbSchema(BaseModel):
#     id:str
#     shop_id:str
#     inventory_id:str
#     variant_id:Optional[str]=None
#     name:str
#     stocks:int
#     expiry_date:Optional[date]=None
#     manufacturing_date:Optional[date]=None




class InventoryBatchDbSchema(BaseModel):
    id:str
    shop_id:str
    inventory_id:str
    variant_id:Optional[str]=None
    name:str
    expiry_date:date
    manufacturing_date:date
    stocks:int



class InventoryVariantDbSchema(BaseModel):
    id:str
    shop_id:str
    inventory_id:str
    name:str
    sell_price:float
    buy_price:float
    stocks:int
    datas:Optional[dict]=None


class InventorySerialNumberDbSchema(BaseModel):
    id:str
    shop_id:str
    inventory_id:str
    batch_id:Optional[str]=None
    variant_id:Optional[str]=None
    serial_numbers:List