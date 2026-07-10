from pydantic import BaseModel
from datetime import date
from typing import Optional,List


class ProductTypeInfosType(BaseModel):
    has_variant: Optional[bool] = False
    has_batch: Optional[bool] = False
    has_serialno: Optional[bool] = False

class ProductBatchExpirationInfosType(BaseModel):
    manufacturing_date:date
    expiry_date:date