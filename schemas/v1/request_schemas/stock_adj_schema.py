from icecream import Optional,List,ic
from pydantic import BaseModel
from core.data_formats.enums.stock_adj_enums import StockAdjustmentTypesEnum



class StockAdjProductsSchema(BaseModel):
    id:str
    barcode:str
    varient_id:Optional[str]=None
    batch_id:Optional[str]=None
    serial_numbers:Optional[List[str]]=None
    name:str
    quantity:int
    type:StockAdjustmentTypesEnum

    model_config={
        "extra":"allow"
    }


class StockAdjCreateMandatoryFields(BaseModel):
    shop_id:str
    products:list[StockAdjProductsSchema]


class StockAdjCreateSchema(BaseModel):
    datas:StockAdjCreateMandatoryFields



class StockAdjUpdateMandatoryFields(BaseModel):
    id:str
    shop_id:str
    type:StockAdjustmentTypesEnum

class StockAdjUpdateSchema(BaseModel):
    datas:StockAdjUpdateMandatoryFields