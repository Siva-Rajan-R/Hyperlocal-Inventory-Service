from enum import Enum

class StockAdjustmentTypesEnum(str,Enum):
    DECREMENT="DECREMENT"
    INCREMENT="INCREMENT"