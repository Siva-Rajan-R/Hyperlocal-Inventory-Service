from icecream import Optional,List,ic
from pydantic import BaseModel,Field
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum
from datetime import date,datetime


class StockAdjInventoryProductSchema(BaseModel):
    inventory_id:Optional[str]=None
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    serialno_id:Optional[str]=None
    serial_numbers:Optional[List]=None
    stocks:int
    type:StockAdjustmentTypesEnum

    datas:Optional[dict]=None

class CreateStockAdjSchema(BaseModel):
    shop_id:str
    adjusted_date:date
    description:str
    products:List[StockAdjInventoryProductSchema]
    datas:Optional[dict]=None


class GetStockAdjByShopIdSchema(BaseModel):
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata
    shop_id:str
    query:Optional[str]=Field(default="",alias="q")
    limit:Optional[int]=Field(default=10,le=100)
    offset:Optional[int]=Field(default=1)

class GetStockAdjByShopIdSchema(BaseModel):
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata
    shop_id:str
    query:Optional[str]=Field(default="",alias="q")
    limit:Optional[int]=Field(default=10,le=100)
    offset:Optional[int]=Field(default=1)

class GetAllStockAdjSchema(BaseModel):
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata
    query:Optional[str]=Field(default="",alias="q")
    limit:Optional[int]=Field(default=10,le=100)
    offset:Optional[int]=Field(default=1)

class GetStockAdjByIdSchema(BaseModel):
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata
    shop_id:str
    id:str

class GetStockAdjByInventoryIdSchema(BaseModel):
    inventory_id:str
    shop_id:str



