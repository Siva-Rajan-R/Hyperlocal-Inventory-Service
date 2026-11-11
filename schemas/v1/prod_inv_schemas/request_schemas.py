from pydantic import BaseModel, model_validator
from typing import Optional,List,Union
from ..product_schemas.request_schemas import CreateProductBatchSchema,CreateProductSchema,CreateProductVariantSchema
from ..inventory_schemas.request_schemas import CreateInventoryPricingSchema,CreateInventoryStockSchema,CreateInventoryStorageLocationSchema
from ..product_schemas.custom_types import ProductBatchExpirationInfosType,ProductTypeInfosType,VariantTypeDefinition
from datetime import date

def _validate_mrp_bounds(sell_price: Optional[float], online_sell_price: Optional[float], mrp: Optional[float], label: str = "") -> None:
    if mrp is not None and float(mrp) > 0:
        mrp_val = float(mrp)
        if sell_price is not None and float(sell_price) > mrp_val:
            raise ValueError(f"Selling price ({sell_price}) cannot exceed MRP ({mrp}){label}.")
        if online_sell_price is not None and float(online_sell_price) > mrp_val:
            raise ValueError(f"Online selling price ({online_sell_price}) cannot exceed MRP ({mrp}){label}.")


def _extract_schema_mrp(mrp: Optional[float], custom_fields: Optional[dict] = None) -> Optional[float]:
    if mrp is not None and float(mrp) > 0:
        return float(mrp)
    if isinstance(custom_fields, dict) and custom_fields.get("mrp") is not None:
        try:
            cf_mrp = float(custom_fields["mrp"])
            if cf_mrp > 0:
                return cf_mrp
        except (TypeError, ValueError):
            pass
    return mrp


class CreateProdInvVariantType(BaseModel):
    name:str
    storage_location:Optional[str]=None
    reorder_point:Optional[float]=5
    online_reorder_point:Optional[float]=0.0
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    online_sell_price:Optional[float]=None
    mrp:Optional[float]=None
    discount_percent:Optional[float]=None
    offer_valid_until:Optional[Union[str, date]]=None
    visible_online:Optional[bool]=False
    sku: Optional[str] = None
    barcode: Optional[str] = None
    sku: Optional[str] = None
    is_active: Optional[bool] = None
    stocks: Optional[float] = None

    @model_validator(mode='after')
    def validate_prices(self):
        _validate_mrp_bounds(self.sell_price, self.online_sell_price, self.mrp, f" for variant '{self.name}'")
        return self

class UpdateProdInvVariantType(BaseModel):
    id: Optional[str] = None
    pricing_id: Optional[str] = None
    storage_location_id: Optional[str] = None
    reorder_point_id: Optional[str] = None
    name: Optional[str] = None
    buy_price: Optional[float] = None
    sell_price: Optional[float] = None
    online_sell_price: Optional[float] = None
    mrp: Optional[float] = None
    discount_percent: Optional[float] = None
    offer_valid_until: Optional[Union[str, date]] = None
    storage_location: Optional[str] = None
    reorder_point: Optional[float] = None
    online_reorder_point: Optional[float] = None
    barcode: Optional[str] = None
    stocks: Optional[float] = None

    @model_validator(mode='after')
    def validate_prices(self):
        _validate_mrp_bounds(self.sell_price, self.online_sell_price, self.mrp, " for variant")
        return self


class CreateProdInvBatchType(BaseModel):
    name:str
    expiry_date:date
    manufacturing_date:date
    visible_online:Optional[bool]=False



class CreateProdInvSchema(BaseModel):
    shop_id:str
    category_id:str
    unit_id:str
    name:str
    brand:Optional[str]=None
    description:str
    barcode:Optional[str]=None
    type_infos:ProductTypeInfosType
    have_tracking:bool
    variant_types: Optional[List[VariantTypeDefinition]] = None
    variant_infos:Optional[List[CreateProdInvVariantType]]=None
    storage_location:Optional[str]=None
    buy_price:Optional[float]=None
    gst:Optional[str]="0%"
    sell_price:Optional[float]=None
    online_sell_price:Optional[float]=None
    mrp:Optional[float]=None
    discount_percent:Optional[float]=None
    offer_valid_until:Optional[Union[str, date]]=None
    reorder_point:Optional[float]=5
    online_reorder_point:Optional[float]=0.0
    custom_fields:Optional[dict]={}
    additional_infos:Optional[dict]=None
    return_policy:Optional[dict]=None
    visible_online:Optional[bool]=False
    sku: Optional[str] = None
    stocks: Optional[float] = None
    is_active: Optional[bool] = False

    @model_validator(mode='after')
    def validate_prices(self):
        base_mrp = _extract_schema_mrp(self.mrp, self.custom_fields)
        _validate_mrp_bounds(self.sell_price, self.online_sell_price, base_mrp)
        if self.variant_infos:
            for variant in self.variant_infos:
                effective_mrp = variant.mrp if (variant.mrp is not None and variant.mrp > 0) else base_mrp
                _validate_mrp_bounds(variant.sell_price, variant.online_sell_price, effective_mrp, f" for variant '{variant.name}'")
        return self



class UpdateProdInvSchema(BaseModel):
    id: str
    shop_id: str
    category_id: Optional[str] = None
    unit_id: Optional[str] = None
    name: Optional[str] = None
    brand: Optional[str] = None
    description: Optional[str] = None
    type_infos: Optional[ProductTypeInfosType] = None
    have_tracking: Optional[bool] = None
    variant_types: Optional[List[VariantTypeDefinition]] = None
    variant_infos: Optional[List[UpdateProdInvVariantType]] = None
    storage_location: Optional[str] = None
    buy_price: Optional[float] = None
    sell_price: Optional[float] = None
    online_sell_price: Optional[float] = None
    mrp: Optional[float] = None
    discount_percent: Optional[float] = None
    offer_valid_until: Optional[Union[str, date]] = None
    reorder_point: Optional[float] = None
    online_reorder_point: Optional[float] = None
    custom_fields: Optional[dict] = None
    additional_infos: Optional[dict] = None
    return_policy: Optional[dict] = None
    visible_online: Optional[bool] = None
    gst: Optional[str] = None
    barcode: Optional[str] = None

    @model_validator(mode='after')
    def validate_prices(self):
        base_mrp = _extract_schema_mrp(self.mrp, self.custom_fields)
        _validate_mrp_bounds(self.sell_price, self.online_sell_price, base_mrp)
        if self.variant_infos:
            for variant in self.variant_infos:
                effective_mrp = variant.mrp if (variant.mrp is not None and variant.mrp > 0) else base_mrp
                _validate_mrp_bounds(variant.sell_price, variant.online_sell_price, effective_mrp, f" for variant '{variant.name or variant.id}'")
        return self


class DeleteProdInvSchema(BaseModel):
    id:str
    shop_id:str


class CreateProdInvBatchSerialnoBatchInfosType(BaseModel):
    name:str
    expiration_infos:ProductBatchExpirationInfosType
    visible_online:Optional[bool]=False


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
    online_sell_price:Optional[float]=0.0


class CreateInvAllStlInfosType(BaseModel):
    id:Optional[str]=None
    name:Optional[str]=None

class CreateInvAllRopInfosType(BaseModel):
    id:Optional[str]=None
    reorder_point:float
    online_reorder_point:Optional[float]=0.0



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
    gst:Optional[str]=None
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
    name:Optional[str]=None

    
class UpdateAllProdInvSchema(BaseModel):
    product_id:str
    shop_id:str
    variant_id:Optional[str]=None
    batch_infos:Optional[BatchInfosProdInvType]=None
    serialno_infos:Optional[List[SerialnoInfosProdInvType]]=None
    stocks:float
    storage_location:Optional[str]=None
    reorder_point:Optional[float]=None
    online_reorder_point:Optional[float]=None
    name:Optional[str]=None
    gst:Optional[str]=None
    description:Optional[str]=None
    buy_price:Optional[float]=None
    sell_price:Optional[float]=None
    online_sell_price:Optional[float]=None
    mrp:Optional[float]=None
    type:str

    @model_validator(mode='after')
    def validate_prices(self):
        _validate_mrp_bounds(self.sell_price, self.online_sell_price, self.mrp)
        return self
    entity_name:str
    entity_id:Optional[str]=None
    ui_id:Optional[str]=None
    purchase_id:Optional[str]=None
    order_id:Optional[str]=None
    create_stock_mov_adj:bool=False
    added_by: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    user_email: Optional[str] = None
    user_role: Optional[str] = None
    user_info: Optional[dict] = None
    user_infos: Optional[dict] = None
