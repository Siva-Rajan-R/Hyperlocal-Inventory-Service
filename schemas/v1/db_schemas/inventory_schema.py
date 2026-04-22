from pydantic import BaseModel
from typing import List,Optional
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum



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
    stocks:Optional[int]=None
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    datas:Optional[dict]={}

    model_config={
        'use_enum_values':True
    }