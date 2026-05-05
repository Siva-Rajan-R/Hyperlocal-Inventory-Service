from pydantic import BaseModel
from typing import Optional,List
from core.data_formats.enums.purchase_enums import PurchaseCalcultionDividedValue,PurchaseTypeEnums,PurchaseViewsEnums
from core.data_formats.typed_dicts.purchase_typdict import PurchaseCalculationsTypDict,PurchaseAdditionalCharges


class CreatePurchaseDbSchema(BaseModel):
    id:str
    shop_id:str
    type:PurchaseTypeEnums
    purchase_view:bool
    supplier_id:str
    calculations:PurchaseCalculationsTypDict
    additional_charges:PurchaseAdditionalCharges
    datas:Optional[dict]=None

class UpdatePurchaseDbSchema(BaseModel):
    id:str
    shop_id:str
    type:Optional[PurchaseTypeEnums]=None
    purchase_view:Optional[bool]=None
    calculations:Optional[PurchaseCalculationsTypDict]=None
    additional_charges:Optional[PurchaseAdditionalCharges]=None
    datas:Optional[dict]=None
