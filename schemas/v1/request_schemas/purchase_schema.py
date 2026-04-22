from pydantic import BaseModel
from typing import Optional,List,Literal
from core.data_formats.enums.purchase_enums import PurchaseTypeEnums

class PurchaseProductsSchema(BaseModel):
    id:str
    barcode:str
    name:str
    quantity:int
    buy_price:float
    sell_price:float

    model_config={
        "extra":"allow"
    }


class PurchaseCreateMandatoryFields(BaseModel):
    shop_id:str
    type:PurchaseTypeEnums
    products:List[PurchaseProductsSchema]
    supplier_id:str
    supplier_name:str

    model_config ={
        "extra":"allow"
    }

class CreatePurchaseSchema(BaseModel):
    datas:PurchaseCreateMandatoryFields


class PurchaseUpdateMandatoryFields(BaseModel):
    id:str
    shop_id:str
    type:Literal['PO_UPDATE']
    products:List[PurchaseProductsSchema]

    model_config ={
        "extra":"allow"
    }

class UpdatePurchaseSchema(BaseModel):
    datas:PurchaseUpdateMandatoryFields
