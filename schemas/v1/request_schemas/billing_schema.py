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

    