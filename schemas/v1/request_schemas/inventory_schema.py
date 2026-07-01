from pydantic import BaseModel, Field
from typing import Optional

class GetAllInventorySchema(BaseModel):
    limit: int = 10
    offset: int = 1
    is_active: Optional[bool] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    query: Optional[str] = None
    shop_id: Optional[str] = None

class GetInventoryByShopIdSchema(BaseModel):
    shop_id: str
    limit: int = 10
    offset: int = 1
    is_active: Optional[bool] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    query: Optional[str] = None

class GetInventoryByIdSchema(BaseModel):
    id: str
    shop_id: str
