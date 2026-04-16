from pydantic import BaseModel
from typing import Optional,List
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums


class CreatePurchaseDbSchema(BaseModel):
    id:str
    shop_id:str
    added_by:str
    type:PurchaseTypeEnums
    purchase_view:bool
    datas:dict

class UpdatePurchaseDbSchema(BaseModel):
    id:str
    shop_id:str
    type:PurchaseTypeEnums
    purchase_view:Optional[bool]=None
    datas:dict
