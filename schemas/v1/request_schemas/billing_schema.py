from pydantic import BaseModel
from typing import Optional,List,Dict


class BillingProductSchema(BaseModel):
    id:str
    variant_id:Optional[str]=None
    batch_id:Optional[str]=None
    serialno_id:Optional[str]=None
    serial_numbers:Optional[List[str]]=[]
    datas:Optional[dict]=None
    quantity:float


class BillingRefundItemsInfoSchema(BaseModel):
    item_id:str
    inventory_id:str
    quantity:float
    reason:str
    serial_numbers:Optional[List[str]]=None



class CreateBillingSchema(BaseModel):
    products:List[BillingProductSchema]
    shop_id:str
    payments:dict
    customer_id:Optional[str]=None
    datas:Optional[dict]=None
    status:Optional[str]="COMPLETED"


class CreateBillingReturnBulkSchema(BaseModel):
    order_id:str
    shop_id:str
    customer_id:Optional[str]=None
    payments:Optional[dict]=None
    items:List[BillingRefundItemsInfoSchema]

class CreateBillingReturnSchema(BaseModel):
    shop_id:str
    customer_id:Optional[str]=None
    payments:Optional[dict]=None
    order_id:str
    item_id:str
    


class CreateBillingExchangeSchema(BaseModel):
    shop_id:str
    customer_id:Optional[str]=None
    order_id:str
    item_id:str
    payments:dict
    product:BillingProductSchema
    
class CreateBillingBulkExchangeSchema(BaseModel):
    shop_id:str
    customer_id:Optional[str]=None
    order_id:str
    items:List[BillingRefundItemsInfoSchema]
    payments:dict
    products:List[BillingProductSchema]
    