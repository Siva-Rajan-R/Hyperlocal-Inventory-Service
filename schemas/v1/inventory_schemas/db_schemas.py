from pydantic import BaseModel
from datetime import date
from typing import Optional,List,Dict


# STOCKS
class CreateInventoryStockDbSchema(BaseModel):
    id:str
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    physical_stocks:float
    reserved_stocks:Optional[float]=None


class UpdateInventoryStockDbSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    type:Optional[str]="DIRECT"
    physical_stocks:Optional[float]=None
    reserved_stocks:Optional[float]=None




# PRICING
class CreateInventoryPricingDbSchema(BaseModel):
    id:str
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    buy_price:float
    sell_price:float
    online_sell_price:float = 0.0

class UpdateInventoryPricingDbSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    buy_price:float
    sell_price:float
    online_sell_price:Optional[float]=None




# STORAGE LOCATION
class CreateInventoryStorageLocationDbSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:str

class UpdateInventoryStorageLocationDbSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:Optional[str]=None


# REORDER POINT
class CreateInventoryReorderPointDbSchema(BaseModel):
    id:str
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    reorder_point:float
    online_reorder_point:float = 0.0

class UpdateInventoryReorderPointDbSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    reorder_point:Optional[float]=None
    online_reorder_point:Optional[float]=None




