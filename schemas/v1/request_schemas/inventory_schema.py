from pydantic import BaseModel,Field
from typing import List,Optional,Dict
from datetime import date,datetime
from core.data_formats.enums.inventory_enums import InventoryProductCategoryEnum
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum


class OptionalInventorySchema(BaseModel):
    ...




class InventoryBatchSchema(BaseModel):
    name:str
    expiry_data:date
    mfg_date:date



class InventoryVariantSchema(BaseModel):
    name:str
    sell_price:float
    buy_price:float
    stocks:int
    serial_numbers:Optional[List]=None
    batch:Optional[InventoryBatchSchema]=None
    datas:Optional[dict]=None


class CreateInventorySchema(BaseModel):
    shop_id:str
    name:str
    category:str
    description:str
    buy_price:float
    sell_price:float
    stocks:Optional[int]=None
    barcode:str

    has_variant:Optional[bool]=None
    has_serialno:Optional[bool]=None
    has_batch:Optional[bool]=None

    variants:Optional[List[InventoryVariantSchema]]=None
    serial_numbers:Optional[List]=None
    batch:Optional[InventoryBatchSchema]=None

    datas:Optional[dict]=None

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

    