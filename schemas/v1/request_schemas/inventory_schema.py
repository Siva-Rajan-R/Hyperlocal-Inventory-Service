from pydantic import BaseModel
from typing import List,Optional
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum



class AddInventorySchema(BaseModel):
    shop_id:str
    barcode:str
    stocks:int
    buy_price:float
    sell_price:float
    datas:Optional[dict]={}


class UpdateInventorySchema(BaseModel):
    shop_id:str
    barcode:str
    qty:int
    