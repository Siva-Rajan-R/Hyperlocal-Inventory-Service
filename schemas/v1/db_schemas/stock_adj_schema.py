from pydantic import BaseModel
from typing import Optional,List
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum,StockAdjustmentMovementType
from datetime import date,datetime

class CreateStockAdjDbSchema(BaseModel):
    id:str
    shop_id:str
    adjusted_date:date
    movement_type:StockAdjustmentMovementType
    description:str
    datas:Optional[dict]=None


class StockAdjUpdateDbSchema(BaseModel):
    id:str
    shop_id:str
    datas:dict