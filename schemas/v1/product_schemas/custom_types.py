from pydantic import BaseModel
from datetime import date
from typing import Optional,List


class ProductTypeInfosType(BaseModel):
    has_batch:bool
    has_variant:bool
    has_serialno:bool

class ProductBatchExpirationInfosType(BaseModel):
    manufacturing_date:date
    expiry_date:date