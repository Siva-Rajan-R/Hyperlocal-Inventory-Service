from ..main import BASE
from sqlalchemy import Column,String,ForeignKey,Integer,TIMESTAMP,func,Float
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

class Inventory(BASE):
    __tablename__="inventory"
    id=Column(String,primary_key=True)
    shop_id=Column(String,nullable=False)
    added_by=Column(String,nullable=False)
    stocks=Column(Integer,nullable=False)
    buy_price=Column(Float,nullable=False)
    sell_price=Column(Float,nullable=False)
    barcode=Column(String,nullable=False,unique=True)
    datas=Column(JSONB)
    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())

class StockAdjustments(BASE):
    __tablename__="stock_adjustments"
    id=Column(String,primary_key=True)
    shop_id=Column(String,nullable=False)
    datas=Column(JSONB)

    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())