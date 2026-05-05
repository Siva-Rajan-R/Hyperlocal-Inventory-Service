from ..main import BASE
from sqlalchemy import Column,String,ForeignKey,Integer,TIMESTAMP,func,Float,BigInteger,Identity,Boolean,ARRAY,Date
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

class Inventory(BASE):
    __tablename__="inventory"
    id=Column(String,primary_key=True)
    ui_id=Column(BigInteger,Identity(always=True),autoincrement=True)
    sequence_id=Column(BigInteger,Identity(always=True),autoincrement=True)
    shop_id=Column(String,nullable=False)
    added_by=Column(String,nullable=False)
    name=Column(String,nullable=True)
    description=Column(String,nullable=True)
    category=Column(String,nullable=False)
    stocks=Column(Integer,nullable=False)
    buy_price=Column(Float,nullable=False)
    sell_price=Column(Float,nullable=False)
    barcode=Column(String,nullable=False,unique=True)
    has_variant=Column(Boolean,nullable=False)
    has_batch=Column(Boolean,nullable=False)
    has_serialno=Column(Boolean,nullable=False)
    datas=Column(JSONB)
    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())

class InventoryVariants(BASE):
    __tablename__="inventory_variants"
    id=Column(String,primary_key=True)
    shop_id=Column(String,nullable=False)
    inventory_id=Column(String,nullable=False)
    name=Column(String,nullable=True)
    stocks=Column(Integer,nullable=False)
    buy_price=Column(Float,nullable=False)
    sell_price=Column(Float,nullable=False)
    datas=Column(JSONB)
    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())


class InventoryBatches(BASE):
    __tablename__="inventory_batches"
    id=Column(String,primary_key=True)
    shop_id=Column(String,nullable=False)
    inventory_id=Column(String,nullable=False)
    variant_id=Column(String,nullable=True)
    stocks=Column(Integer,nullable=False)
    expiry_date=Column(TIMESTAMP(timezone=True),nullable=True)
    manufacturing_date=Column(TIMESTAMP(timezone=True),nullable=True)
    name=Column(String,nullable=True)
    datas=Column(JSONB)
    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())

class InventorySerialNumbers(BASE):
    __tablename__="inventory_serial_numbers"
    id=Column(String,primary_key=True)
    shop_id=Column(String,nullable=False)
    inventory_id=Column(String,nullable=False)
    variant_id=Column(String,nullable=True)
    batch_id=Column(String,nullable=True)
    serial_numbers=Column(ARRAY(String),nullable=False)

    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())


class StockAdjustments(BASE):
    __tablename__="stock_adjustments"
    id=Column(String,primary_key=True)
    ui_id=Column(BigInteger,Identity(always=True),autoincrement=True)
    sequence_id=Column(BigInteger,Identity(always=True),autoincrement=True)
    shop_id=Column(String,nullable=False)
    description=Column(String,nullable=False)
    adjusted_date=Column(Date,nullable=False)
    datas=Column(JSONB)

    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())


class StockAdjustmentInventoryProducts(BASE):
    __tablename__="stockadjustment_inventory_products"
    id=Column(BigInteger,primary_key=True,autoincrement=True)
    stockadjustment_id=Column(String,nullable=False)
    inventory_id=Column(String,nullable=False)
    variant_id=Column(String,nullable=True)
    batch_id=Column(String,nullable=True)
    stocks=Column(BigInteger,nullable=False)
    type=Column(String,nullable=False)

    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())
    updated_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now(),onupdate=func.now())