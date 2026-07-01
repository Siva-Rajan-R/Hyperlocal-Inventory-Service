from pydantic import BaseModel
from datetime import date
from typing import Optional,List,Dict
from core.data_formats.enums.product_enums import ProductSerialnoStatusEnums
from .custom_types import ProductBatchExpirationInfosType,ProductTypeInfosType



# PRODUCT
class CreateProductDbSchema(BaseModel):
    id:str
    ui_id:str
    shop_id:str
    sku:str
    barcode:Optional[str]=None
    category_id:str
    unit_id:str
    name:str
    gst:Optional[str]="0%"
    description:str
    type_infos:ProductTypeInfosType
    have_tracking:bool
    is_active:bool


class UpdateProductDbSchema(BaseModel):
    id:str
    shop_id:Optional[str]=None
    category_id:Optional[str]=None
    unit_id:Optional[str]=None
    sku:Optional[str]=None
    barcode:Optional[str]=None
    name:Optional[str]=None
    description:Optional[str]=None
    type_infos:Optional[ProductTypeInfosType]=None
    have_tracking:Optional[bool]=None
    is_active:Optional[bool]=None
    gst:Optional[str]="0%"


class DeleteProductDbSchema(BaseModel):
    id:str
    shop_id:str

# VARIANT
class CreateProductVariantDbSchema(BaseModel):
    id:str
    product_id:str
    shop_id:str
    name:str

class UpdateProductVariantDbSchema(BaseModel):
    id:str
    shop_id:str
    name:Optional[str]=None

# BATCH
class CreateProductBatchDbSchema(BaseModel):
    id:str
    product_id:str
    shop_id:str
    variant_id:Optional[str]=None
    name:str
    expiration_infos:ProductBatchExpirationInfosType

class UpdateProductBatchDbSchema(BaseModel):
    id:str
    shop_id:str
    variant_id:Optional[str]=None
    name:Optional[str]=None
    expiration_infos:Optional[ProductBatchExpirationInfosType]=None


# SERIAL NUMBERS
class CreateProductSerialnoDbSchema(BaseModel):
    id:str
    product_id:str
    shop_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:str
    status:Optional[ProductSerialnoStatusEnums]=ProductSerialnoStatusEnums.AVAILABLE

class UpdateProductSerialnoDbSchema(BaseModel):
    id:str
    shop_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:Optional[str]=None
    status:Optional[ProductSerialnoStatusEnums]=ProductSerialnoStatusEnums.AVAILABLE





