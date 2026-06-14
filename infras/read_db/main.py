from motor.motor_asyncio import AsyncIOMotorClient
from core.configs.settings_config import SETTINGS
import asyncio
from icecream import ic

MONGO_CLIENT=AsyncIOMotorClient(SETTINGS.MONGO_DB_URL)


DB=MONGO_CLIENT["InventoryServiceDb"]

INVENTORY_COLLECTION=DB['inventory_collections']
PURCHAESE_COLLECTION=DB['purchase_collections']
PURCHASE_STATS_COLLECTION=DB['purchase_stats_collections']
STOCK_MOVEMENT_COLLECTION=DB['stock_movement_collections']
STOCK_MOVEMENT_STATS_COLLECTION=DB['stock_movement_stats_collections']
SALES_COLLECTION=DB['sales_collections']
SUPPLIER_STATS_COLLECTION=DB['supplier_stats_collections']