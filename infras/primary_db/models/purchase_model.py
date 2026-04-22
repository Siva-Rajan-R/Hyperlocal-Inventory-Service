from ..main import BASE
from sqlalchemy import Column,String,ForeignKey,Integer,TIMESTAMP,func,Float,Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

class Purchase(BASE):
    __tablename__="purchase"
    id=Column(String,primary_key=True)
    added_by=Column(String,nullable=False)
    shop_id=Column(String,nullable=False)
    type=Column(String,nullable=False)
    purchase_view=Column(Boolean,nullable=False)
    
    datas=Column(JSONB,nullable=False)
    created_at=Column(TIMESTAMP(timezone=True),nullable=False,server_default=func.now())