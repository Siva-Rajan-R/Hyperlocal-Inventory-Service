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


class UpdateInventoryDbSchema(BaseModel):
    id:str
    barcode:str
    shop_id:str
    stocks:Optional[int]=None
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    image_urls:Optional[List[str]]=None
    product_name:Optional[str]=None
    product_description:Optional[str]=None
    product_category:Optional[InventoryProductCategoryEnum]=None
    offer_online:Optional[str]=None
    offer_offline:Optional[str]=None
    offer_type:Optional[str]=None

    model_config={
        'use_enum_values':True
    }