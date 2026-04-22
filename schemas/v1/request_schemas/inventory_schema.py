from pydantic import BaseModel
from typing import List,Optional
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum



class InventoryCreateMandatoryFields(BaseModel):
    shop_id:str
    barcode:str
    stocks:int
    buy_price:float
    sell_price:float
    name:str
    description:str
    category:str

    model_config={
        "extra":"allow"
    }

class AddInventorySchema(BaseModel):
    datas:InventoryCreateMandatoryFields


class InventoryUpdateMandatoryFields(BaseModel):
    id:str
    shop_id:str
    barcode:str
    stocks:int
    buy_price:float
    sell_price:float
    name:str
    description:str
    category:str

    model_config={
        "extra":"allow"
    }

class UpdateInventorySchema(BaseModel):
    datas:InventoryUpdateMandatoryFields
    