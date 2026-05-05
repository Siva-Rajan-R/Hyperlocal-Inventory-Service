from ..main import BASE
from sqlalchemy import Column,String,ForeignKey,Integer,TIMESTAMP,func,Float,Boolean,BigInteger,Identity
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

class Purchase(BASE):
    __tablename__="purchase"
    id=Column(String,primary_key=True)
    sequence_id=Column(BigInteger,Identity(always=True),nullable=False)
    ui_id=Column(BigInteger,Identity(always=True),nullable=False)
    shop_id=Column(String,nullable=False)
    supplier_id=Column(String,nullable=False)
    type=Column(String,nullable=False)
    purchase_view=Column(Boolean,nullable=False)
    calculations=Column(JSONB,nullable=False)
    additional_charges=Column(JSONB,nullable=False)
    datas=Column(JSONB,nullable=True)

    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())


class PurchaseInventoryProducts(BASE):
    __tablename__="purchase_inventory_products"
    id=Column(BigInteger,primary_key=True,autoincrement=True)
    purchase_id=Column(String,nullable=False)
    inventory_id=Column(String,nullable=False)
    variant_id=Column(String,nullable=True)
    batch_id=Column(String,nullable=True)
    stocks=Column(BigInteger,nullable=False)
    received_stocks=Column(BigInteger,nullable=False)
    sell_price=Column(Float,nullable=False)
    buy_price=Column(Float,nullable=False)
    margin=Column(Float,nullable=False)

    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())
