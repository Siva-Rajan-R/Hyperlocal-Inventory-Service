from pydantic import BaseModel
from typing import Optional,List,Dict


class BillingProductSchema(BaseModel):
    id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    serialno_id:Optional[str]=None
    serial_numbers:Optional[List[str]]=[]
    quantity:int



class CreateBillingSchema(BaseModel):
    products:List[BillingProductSchema]
    shop_id:str
    payment_method:str
    customer_id:str
    status:Optional[str]="COMPLETED"


class CreateBillingReturnBulkSchema(BaseModel):
    order_id:str
    items_id:List[str]

class CreateBillingReturnSchema(BaseModel):
    order_id:str
    item_id:str


class CreateBillingExchangeSchema(BaseModel):
    shop_id:str
    customer_id:str
    order_id:str
    item_id:str
    payment_method:str
    product:BillingProductSchema
    
class CreateBillingBulkExchangeSchema(BaseModel):
    shop_id:str
    customer_id:str
    order_id:str
    items_id:List[str]
    payment_method:str
    products:List[BillingProductSchema]
    