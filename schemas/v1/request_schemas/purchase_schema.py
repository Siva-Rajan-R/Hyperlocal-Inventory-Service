from pydantic import BaseModel
from typing import Optional,List,Literal
from datetime import date

from core.data_formats.enums.purchase_enums import PurchaseTypeEnums


class BatchesSchema(BaseModel):
    expiry_date:date
    manufacturing_date:date

class PurchaseProductsCreateSchema(BaseModel):
    inventory_id:str
    varient_id:Optional[str]=None
    batch_name:Optional[str]=None
    batches:BatchesSchema
    barcode:str
    name:str
    quantity:int
    buy_price:float
    sell_price:float
    serial_numbers:Optional[List[str]]=[]

    model_config={
        "extra":"allow"
    }

class PurchaseProductsUpdateSchema(BaseModel):
    inventory_id:str
    varient_id:Optional[str]=None
    batch_name:Optional[str]=None
    batches:BatchesSchema
    barcode:str
    name:str
    quantity:int
    received_qty:int
    buy_price:float
    sell_price:float
    serial_numbers:Optional[List[str]]=[]

    model_config={
        "extra":"allow"
    }


class PurchaseCreateMandatoryFields(BaseModel):
    shop_id:str
    type:PurchaseTypeEnums
    products:List[PurchaseProductsCreateSchema]
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
    products:List[PurchaseProductsUpdateSchema]

    model_config ={
        "extra":"allow"
    }

class UpdatePurchaseSchema(BaseModel):
    datas:PurchaseUpdateMandatoryFields
