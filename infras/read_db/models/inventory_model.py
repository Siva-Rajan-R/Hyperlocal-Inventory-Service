from pydantic import BaseModel
from typing import Optional,List
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum


class ReadDbInventoryCreateModel(BaseModel):
    inventory_id:str
    shop_id:str
    barcode:str
    stocks:int
    buy_price:float
    sell_price:float
    image_urls:List[str]=[]
    product_name:str
    product_description:str
    product_category:InventoryProductCategoryEnum
    offer_online:str
    offer_type:str
    offer_offline:str
    added_by:str

class ReadDbInventoryUpdateModel(BaseModel):
    stocks:Optional[int]=None
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    image_urls:Optional[List[str]]=None
    product_name:Optional[str]=None
    offer_online:Optional[str]=None
    offer_type:Optional[str]=None
    offer_offline:Optional[str]=None
    product_description:Optional[str]=None
    product_category:Optional[InventoryProductCategoryEnum]=None


class ReadDbInventoryReadModel(ReadDbInventoryCreateModel):
    ...