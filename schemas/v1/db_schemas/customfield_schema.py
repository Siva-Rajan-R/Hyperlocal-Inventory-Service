from pydantic import BaseModel
from typing import Optional, Any
from ..request_schemas.customfield_schema import GetFieldByShopIdSchema

class CreateCustomFieldDbSchema(BaseModel):
    id: str
    shop_id: str
    category_id: Optional[str] = None
    field_name: str
    label_name: str
    type: str
    required: bool = False
    visible_online: bool = False
    options: Optional[Any] = None
    default_value: Optional[str] = None

class CreateCustomFieldValueDbSchema(BaseModel):
    id: str
    shop_id: str
    product_id: str
    field_id: str
    value: str


class UpdateCustomFieldDbSchema(BaseModel):
    id:str
    shop_id:str
    category_id: Optional[str] = None
    label_name: Optional[str]=None
    type: Optional[str]=None
    required: Optional[bool] = None
    visible_online: Optional[bool] = None
    options: Optional[Any] = None
    default_value: Optional[str] = None

class DeleteCustomFieldDbSchema(BaseModel):
    id:str
    shop_id:str

