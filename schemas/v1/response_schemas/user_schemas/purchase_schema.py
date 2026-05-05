from pydantic import BaseModel
from typing import Optional,List
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums,PurchaseCalcultionDividedValue,PurchaseViewsEnums
from datetime import datetime


class PurchaseGetResponseSchema(BaseModel):
    id:str
    ui_id:int
    shop_id:str
    type:PurchaseTypeEnums
    purchase_view:str
    supplier_id:str
    datas:Optional[dict]={}
    additional_charges:dict
    calculations:dict
    updated_at:datetime
    created_at:datetime

    products:List[dict]