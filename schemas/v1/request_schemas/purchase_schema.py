from pydantic import BaseModel,Field
from typing import Optional,List
from core.data_formats.enums.purchase_enums import PurchaseCalcultionDividedValue,PurchaseTypeEnums,PurchaseViewsEnums
from .inventory_schema import InventoryBatchSchema
from core.data_formats.typed_dicts.purchase_typdict import PurchaseCalculationsTypDict,PurchaseAdditionalCharges
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum


class PurchaseInventoryProductSchema(BaseModel):
    inventory_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    serialno_id:Optional[str]=None
    serial_numbers:Optional[List]=None
    batch:Optional[InventoryBatchSchema]=None
    sell_price:float
    buy_price:float
    margin:float
    stocks:int
    received_stocks:Optional[int]=None


class CreatePurchaseSchema(BaseModel):
    purchase_id:Optional[str]=None
    shop_id:str
    type:PurchaseTypeEnums
    supplier_id:str
    calculations:PurchaseCalculationsTypDict
    additional_charges:PurchaseAdditionalCharges
    datas:Optional[dict]=None
    products:List[PurchaseInventoryProductSchema]

class BulkCheckPurchaseSchema(BaseModel):
    purchase_id:str
    inventory_id:List[str]


class GetPurchaseByShopIdSchema(BaseModel):
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata
    shop_id:str
    view:PurchaseViewsEnums
    query:Optional[str]=Field(default="")
    limit:Optional[int]=Field(default=10,le=100)
    offset:Optional[int]=Field(default=1)

class GetPurchaseByIdSchema(BaseModel):
    id:str
    shop_id:str

class GetPurchaseByInventoryIdSchema(BaseModel):
    inventory_id:str
    shop_id:str



