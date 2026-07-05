from pydantic import BaseModel
from datetime import date
from typing import Optional,List,Dict


# STOCKS
class CreateInventoryStockSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    physical_stocks:float
    reserved_stocks:Optional[float]=None


class UpdateInventoryStockSchema(BaseModel):
    id:str
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    physical_stocks:Optional[float]=None
    reserved_stocks:Optional[float]=None




# PRICING
class CreateInventoryPricingSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    buy_price:float
    sell_price:float

class UpdateInventoryPricingSchema(BaseModel):
    id:str
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    buy_price:float
    sell_price:float




# STORAGE LOCATION
class CreateInventoryStorageLocationSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:str

class UpdateInventoryStorageLocationSchema(BaseModel):
    id:str
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:Optional[str]=None


# REORDER POINT
class CreateInventoryReorderPointSchema(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    reorder_point:float

class UpdateInventoryReorderPointSchema(BaseModel):
    id:str
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    reorder_point:Optional[float]=None





class VerifyInventoryCombinedSchema(BaseModel):
    stocks: List[str] = []
    pricings: List[str] = []
    storage_locations: List[str] = []
    reorder_points: List[str] = []

from datetime import datetime



class SerialnoInfosProdInvType(BaseModel):
    id:Optional[str]=None
    name:str


class ReserveInventorySchema(BaseModel):
    session_id: str
    product_id: str
    variant_id: Optional[str] = None
    batch_id: Optional[str] = None
    serialno_infos:Optional[List[SerialnoInfosProdInvType]]=None
    shop_id: str
    qty: float
    expires_at: datetime

class ReleaseInventorySchema(BaseModel):
    session_id: str

class CommitInventorySchema(BaseModel):
    session_id: str
    entity_name:str
    record_stock:bool=False

class ReleaseItemInventorySchema(BaseModel):
    session_id: str
    product_id: str
    variant_id: Optional[str] = None
    batch_id: Optional[str] = None
