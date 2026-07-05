from pydantic import BaseModel
from typing import Optional,List
from ..product_schemas.request_schemas import CreateProductBatchSchema,CreateProductSchema,CreateProductVariantSchema
from ..inventory_schemas.request_schemas import CreateInventoryPricingSchema,CreateInventoryStockSchema,CreateInventoryStorageLocationSchema
from ..product_schemas.custom_types import ProductBatchExpirationInfosType,ProductTypeInfosType
from datetime import date

class CreateProdInvVariantType(BaseModel):
    name:str
    storage_location:Optional[str]=None
    reorder_point:Optional[float]=5
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None

class UpdateProdInvVariantType(BaseModel):
    id:Optional[str]=None
    pricing_id:Optional[str]=None
    storage_location_id:Optional[str]=None
    reorder_point_id:Optional[str]=None
    name:str
    storage_location:Optional[str]=None
    reorder_point:Optional[float]=5
    buy_price:float
    sell_price:float


class CreateProdInvBatchType(BaseModel):
    name:str
    expiry_date:date
    manufacturing_date:date



class CreateProdInvSchema(BaseModel):
    shop_id:str
    category_id:str
    unit_id:str
    name:str
    description:str
    barcode:Optional[str]=None
    type_infos:ProductTypeInfosType
    have_tracking:bool
    variant_infos:Optional[List[CreateProdInvVariantType]]=None
    storage_location:Optional[str]=None
    buy_price:Optional[float]=None
    gst:Optional[str]="0%"
    sell_price:Optional[float]=None
    reorder_point:Optional[float]=5
    custom_fields:Optional[dict]={}



class UpdateProdInvSchema(BaseModel):
    id:str
    shop_id:str
    category_id:Optional[str]=None
    unit_id:Optional[str]=None
    name:Optional[str]=None
    description:Optional[str]=None
    type_infos:Optional[ProductTypeInfosType]=None
    have_tracking:Optional[bool]=None
    variant_infos:Optional[List[UpdateProdInvVariantType]]=None
    storage_location:Optional[str]=None
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    pricing_id:Optional[str]=None
    storage_location_id:Optional[str]=None
    reorder_point_id:Optional[str]=None
    reorder_point:Optional[float]=5
    custom_fields:Optional[dict]={}


class DeleteProdInvSchema(BaseModel):
    id:str
    shop_id:str


class CreateProdInvBatchSerialnoBatchInfosType(BaseModel):
    name:str
    expiration_infos:ProductBatchExpirationInfosType


class CreateProdInvBatchSerialnoSchema(BaseModel):
    product_id:str
    shop_id:str
    variant_id:Optional[str]=None
    batch_infos:CreateProdInvBatchSerialnoBatchInfosType
    serial_numbers:List[str]
    






class CreateInvAllStocksInfosType(BaseModel):
    id:Optional[str]=None
    type:Optional[str]="DIRECT"
    physical_stocks:float
    reserved_stocks:Optional[float]=None 

class CreateInvAllPricingInfosType(BaseModel):
    id:Optional[str]=None
    buy_price:float
    sell_price:float


class CreateInvAllStlInfosType(BaseModel):
    id:Optional[str]=None
    name:Optional[str]=None

class CreateInvAllRopInfosType(BaseModel):
    id:Optional[str]=None
    reorder_point:float



class CreateInventoryAll(BaseModel):
    shop_id:str
    product_id:str
    gst:Optional[str]="0%"
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    stocks_infos:Optional[CreateInvAllStocksInfosType]=None
    pricing_infos:Optional[CreateInvAllPricingInfosType]=None
    storage_location_infos:Optional[CreateInvAllStlInfosType]=None
    reorder_point_infos:Optional[CreateInvAllRopInfosType]=None


class UpdateInventoryAll(BaseModel):
    shop_id:str
    product_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    gst:Optional[str]="0%"
    stocks_infos:Optional[CreateInvAllStocksInfosType]=None
    pricing_infos:Optional[CreateInvAllPricingInfosType]=None
    storage_location_infos:Optional[CreateInvAllStlInfosType]=None
    reorder_point_infos:Optional[CreateInvAllRopInfosType]=None



class CreateUpdateInvAll(BaseModel):
    create:Optional[List[CreateInventoryAll]]=None
    update:Optional[List[UpdateInventoryAll]]=None


class BatchInfosProdInvType(BaseModel):
    id:Optional[str]=None
    name:Optional[str]=None
    expiry_date:Optional[date]=None
    manufacturing_date:Optional[date]=None

class SerialnoInfosProdInvType(BaseModel):
    id:Optional[str]=None
    name:str

    
class UpdateAllProdInvSchema(BaseModel):
    product_id:str
    shop_id:str
    variant_id:Optional[str]=None
    batch_infos:Optional[BatchInfosProdInvType]=None
    serialno_infos:Optional[List[SerialnoInfosProdInvType]]=None
    stocks:float
    storage_location:Optional[str]=None
    reorder_point:Optional[float]=None
    name:Optional[str]=None
    gst:Optional[str]=None
    description:Optional[str]=None
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    type:str
    entity_name:str
    create_stock_mov_adj:bool=False
