from pydantic import BaseModel,Field
from typing import List,Optional,Dict,Union
from datetime import date,datetime
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum


class OptionalInventorySchema(BaseModel):
    gst:Optional[str]=None
    mrp:Optional[float]=None
    unit:Optional[str]=None
    brand:Optional[str]=None
    storage_location:Optional[str]=None
    is_active:Optional[bool]=None
    reorder_point:Optional[int]=None
    opening_stock:Optional[int]=None
    variant_types:Optional[List[dict]]=None




class InventoryBatchSchema(BaseModel):
    name:str
    expiry_date:Union[date,datetime]
    manufacturing_date:Union[date,datetime]


class InventoryBatchResponseSchema(BaseModel):
    id: str
    name: str
    stocks: int

    expiry_date: Union[date, datetime]
    manufacturing_date: Union[date, datetime]

    serial_numbers: Optional[List] = None



class InventoryVariantSchema(BaseModel):
    name:str
    sell_price:Optional[float]=0.0
    buy_price:Optional[float]=0.0
    stocks:Optional[int]=0
    datas:Optional[dict]=None

class InventoryResponseVariantSchema(BaseModel):
    name:str
    sell_price:float
    buy_price:float
    stocks:int
    serial_numbers:Optional[List]=None
    batches:Optional[List[InventoryBatchSchema]]=None
    datas:Optional[dict]=None


class CreateInventorySchema(BaseModel):
    shop_id:str
    name:str
    category:str
    description:Optional[str]=None
    buy_price:Optional[float]=0.0
    sell_price:Optional[float]=0.0
    stocks:Optional[int]=0
    barcode:Optional[str]=None
    reorder_point:int
    has_variant:Optional[bool]=None
    has_serialno:Optional[bool]=None
    has_batch:Optional[bool]=None

    variants:Optional[List[InventoryVariantSchema]]=None

    datas:Optional[OptionalInventorySchema]=None

class UpdateInventorySchema(BaseModel):
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



class DeleteInventorySchema(BaseModel):
    id:str
    shop_id:str


class GetAllInventorySchema(BaseModel):
    query:str=Field(default="",alias="q")
    limit:int=Field(default=10,le=100)
    offset:int=Field(default=1)
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata

class GetInventoryByShopIdSchema(BaseModel):
    shop_id:str
    query:str=Field(default="",alias="q")
    limit:int=Field(default=10,le=100)
    offset:int=Field(default=1)
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata


class GetInventoryByIdSchema(BaseModel):
    shop_id:str
    id:Optional[str]=None
    barcode:Optional[str]=None
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata

class VerifySchema(BaseModel):
    id:Optional[str]=None
    barcode:Optional[str]=None
    shop_id:str


class BulkCheckInventorySchema(BaseModel):
    shop_id:str
    id:List[str]
    