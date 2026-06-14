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
    reorder_point:int
    stocks:float
    received_stocks:Optional[int]=None
    datas:Optional[dict]=None
    storage_location:Optional[str]=None


class CreatePurchaseSchema(BaseModel):
    purchase_id:Optional[str]=None
    shop_id:str
    type:PurchaseTypeEnums
    supplier_id:str
    calculations:PurchaseCalculationsTypDict
    additional_charges:PurchaseAdditionalCharges
    paid_amount:float
    datas:Optional[dict]=None
    products:List[PurchaseInventoryProductSchema]

class BulkCheckPurchaseSchema(BaseModel):
    purchase_id:str
    inventory_id:List[str]


class GetPurchaseByShopIdSchema(BaseModel):
    timezone:Optional[TimeZoneEnum]=TimeZoneEnum.Asia_Kolkata
    shop_id:str
    view:PurchaseViewsEnums=PurchaseViewsEnums.PO_VIEW
    query:Optional[str]=Field(default="")
    limit:Optional[int]=Field(default=10,le=100)
    offset:Optional[int]=Field(default=1)
    from_date:Optional[str]=None
    to_date:Optional[str]=None
    type:Optional[str]=None
    supplier_id:Optional[str]=None

class GetPurchaseByIdSchema(BaseModel):
    id:str
    shop_id:str

class GetPurchaseByInventoryIdSchema(BaseModel):
    inventory_id:str
    shop_id:str


class GetPurchaseBySupplierIdSchema(BaseModel):
    supplier_id:str
    shop_id:str