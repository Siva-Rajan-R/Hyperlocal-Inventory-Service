from pydantic import BaseModel
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum

class StockAdjCreateDbSchema(BaseModel):
    id:str
    shop_id:str
    datas:dict



class StockAdjUpdateDbSchema(BaseModel):
    id:str
    shop_id:str
    datas:dict