from pydantic import BaseModel
from datetime import date
from typing import Optional,List,Dict
from core.data_formats.enums.product_enums import ProductSerialnoStatusEnums
from .custom_types import ProductBatchExpirationInfosType,ProductTypeInfosType



# PRODUCT
class CreateProductSchema(BaseModel):
    shop_id:str
    category_id:str
    unit_id:str
    name:str
    brand:Optional[str]=None
    gst:Optional[str]="0%"
    barcode:str
    description:str
    type_infos:ProductTypeInfosType
    have_tracking:bool
    reorder_point:Optional[float]=0
    visible_online:Optional[bool]=False
    sku: Optional[str] = None


class UpdateProductSchema(BaseModel):
    id:str
    shop_id:Optional[str]=None
    category_id:Optional[str]=None
    unit_id:Optional[str]=None
    name:Optional[str]=None
    brand:Optional[str]=None
    barcode:Optional[str]=None
    description:Optional[str]=None
    type_infos:Optional[ProductTypeInfosType]=None
    have_tracking:Optional[bool]=None
    reorder_point:Optional[float]=0
    gst:Optional[str]="0%"
    visible_online:Optional[bool]=None
    sku: Optional[str] = None


class DeleteProductSchema(BaseModel):
    id:str
    shop_id:str

# VARIANT
class CreateProductVariantSchema(BaseModel):
    product_id:str
    shop_id:str
    name:str
    visible_online:Optional[bool]=False
    sku: Optional[str] = None
    barcode: Optional[str] = None

class UpdateProductVariantSchema(BaseModel):
    id:str
    shop_id:str
    name:Optional[str]=None
    visible_online:Optional[bool]=None
    sku: Optional[str] = None
    barcode: Optional[str] = None

# BATCH
class CreateProductBatchSchema(BaseModel):
    product_id:str
    shop_id:str
    variant_id:Optional[str]=None
    name:str
    expiration_infos:ProductBatchExpirationInfosType
    visible_online:Optional[bool]=False

class UpdateProductBatchSchema(BaseModel):
    id:str
    shop_id:str
    variant_id:Optional[str]=None
    name:Optional[str]=None
    expiration_infos:Optional[ProductBatchExpirationInfosType]=None
    visible_online:Optional[bool]=None


# SERIAL NUMBERS
class CreateProductSerialnoSchema(BaseModel):
    product_id:str
    shop_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:str
    status:Optional[ProductSerialnoStatusEnums]=ProductSerialnoStatusEnums.AVAILABLE
    visible_online:Optional[bool]=False

class UpdateProductSerialnoSchema(BaseModel):
    id:str
    shop_id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    name:Optional[str]=None
    status:Optional[ProductSerialnoStatusEnums]=ProductSerialnoStatusEnums.AVAILABLE
    visible_online:Optional[bool]=None



class GetAllProductSchema(BaseModel):
    query:Optional[str]=None
    limit:Optional[int]=10
    offset:Optional[int]=1
    active:Optional[bool]=None
    include_serialno:Optional[bool]=False
    visible_online:Optional[bool]=None

class GetProductsByShopId(BaseModel):
    query:Optional[str]=None
    limit:Optional[int]=10
    offset:Optional[int]=1
    include_serialno:Optional[bool]=False
    active:Optional[bool]=None
    shop_id:str
    visible_online:Optional[bool]=None

class GetProductsById(BaseModel):
    shop_id:str
    id:str
    include_serialno:Optional[bool]=False
    active:Optional[bool]=None
    visible_online:Optional[bool]=None


class GetBulkProductsById(BaseModel):
    shop_id:Optional[str]=None
    include_serialno:Optional[bool]=False
    active:Optional[bool]=None
    id:List[str]
    visible_online:Optional[bool]=None




class VerifyCombinedSchema(BaseModel):
    products:List[str]
    variants:List[str]
    serialnos:List[str]
    batches:List[str]





