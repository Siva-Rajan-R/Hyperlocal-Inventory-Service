from pydantic import BaseModel
from typing import Optional,List
from datetime import date,datetime

class ProdInvReadModelCategoryInfosType(BaseModel):
    id:str
    name:str

class ProdInvReadModelUnitInfosType(BaseModel):
    id:str
    name:str

class ProdInvReadModelVariantInfosType(BaseModel):
    id:str
    name:str

class ProdInvReadModelSerialnoInfosType(BaseModel):
    id:str
    name:str

class ProdInvReadModelStockInfosType(BaseModel):
    id:str
    physical_stocks:float
    reserved_stocks:float
    available_Stocks:float

class ProdInvReadModelPricingInfosType(BaseModel):
    id:str
    buy_price:float
    sell_price:float

class ProdInvReadModelStorageLocationInfosType(BaseModel):
    id:str
    name:str

class ProdInvReadModelReorderPointInfosType(BaseModel):
    id:str
    reorder_point:float

class ProdInvReadModelBatchInfosType(BaseModel):
    id:str
    name:str
    expiry_data:date
    manufacturing_date:date

class ProdInvReadModelTypeInfosType(BaseModel):
    have_variant: bool
    have_batch: bool
    have_serialno: bool

class ProdInvReadModelInventoryUnitsType(BaseModel):
    variant_infos:Optional[ProdInvReadModelVariantInfosType]=None
    serialno_infos:Optional[List[ProdInvReadModelSerialnoInfosType]]=None
    batch_infos:Optional[ProdInvReadModelBatchInfosType]=None
    stock_infos:ProdInvReadModelStockInfosType
    pricing_infos:ProdInvReadModelPricingInfosType
    storage_location_infos:Optional[ProdInvReadModelStorageLocationInfosType]=None
    reorder_point_infos:ProdInvReadModelReorderPointInfosType



class ProdInvReadModel(BaseModel):
    id:str
    ui_id:str
    shop_id:str
    sku:str
    barcode:str
    name:str
    description:str
    category_infos:ProdInvReadModelCategoryInfosType
    unit_infos:ProdInvReadModelUnitInfosType
    inventory_units:List[ProdInvReadModelInventoryUnitsType]
    type_infos:ProdInvReadModelTypeInfosType
    is_active:bool
    have_tracking:bool
    gst:str
    created_at:datetime
    updated_at:datetime