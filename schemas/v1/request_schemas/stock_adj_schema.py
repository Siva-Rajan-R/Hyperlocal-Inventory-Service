from pydantic import BaseModel
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum

class StockAdjCreateSchema(BaseModel):
    shop_id:str
    datas:dict


class StockAdjUpdateSchema(BaseModel):
    id:str
    shop_id:str
    datas:dict