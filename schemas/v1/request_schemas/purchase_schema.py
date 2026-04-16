from pydantic import BaseModel
from typing import Optional,List
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums

class CreatePurchaseSchema(BaseModel):
    shop_id:str
    type:PurchaseTypeEnums
    datas:dict

class UpdatePurchaseSchema(BaseModel):
    id:str
    shop_id:str
    type:PurchaseTypeEnums
    datas:dict
