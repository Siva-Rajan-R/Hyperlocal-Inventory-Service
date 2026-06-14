from datetime import datetime, date
from typing import List, Optional

from pydantic import BaseModel, Field


class SerialInfo(BaseModel):
    serialno_id: str
    serial_numbers: List[str] = []


class VariantInfo(BaseModel):
    variant_id: str
    variant_name: str


class BatchInfo(BaseModel):
    batch_id: str
    batch_name: str
    mfg_date: Optional[datetime] = None
    exp_date: Optional[datetime] = None


class PurchaseProduct(BaseModel):
    inventory_id: str
    ui_id: str
    name: str
    sell_price:float=0
    buy_price:float=0

    reorder_point:int=0
    stocks_before: float = 0
    stocks_added: float = 0
    stocks_after: float = 0

    total_amount: float = 0
    
    variant: Optional[VariantInfo] = None
    batch: Optional[BatchInfo] = None
    serial_info: Optional[SerialInfo] = None

    storage_location: str
    gst: Optional[str] = None


class SupplierInfo(BaseModel):
    supplier_id: str
    supplier_name: str



class PurchaseReadModel(BaseModel):
    purchase_id: str
    ui_id: str
    invoice_no: str
    shop_id: str

    purchase_date: datetime

    supplier: SupplierInfo

    
    total_cost: float = 0.0
    total_items: int = 0
    total_quantity: int = 0

    paid_amount: float = 0.0
    payment_status: str = "completed"
    transport_charge: float = 0.0
    other_charges: float = 0.0
    calculations: dict = {}

    products: List[PurchaseProduct] = []

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PurchaseStatsReadModel(BaseModel):
    shop_id: str
    total_purchase_count: int = 0
    total_purchase_value: float = 0.0
    outstanding_counts: int = 0
    outstanding_value: float = 0.0
    complete_counts: int = 0
    completed_value: float = 0.0
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)