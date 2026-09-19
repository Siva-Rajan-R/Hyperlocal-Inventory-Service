import httpx
from icecream import ic
from typing import Dict, Any, Optional
import os
from dotenv import load_dotenv
load_dotenv()

from infras.read_db.main import MONGO_CLIENT

SHOPEMP_SERVICE_URL = os.getenv("SHOPEMP_SERVICE_URL", "http://127.0.0.1:8001")


async def is_initial_stock_imported(shop_id: str) -> bool:
    if not shop_id:
        return False

    # 1. First check MongoDB directly (fastest, shared mongo)
    try:
        shops_collection = MONGO_CLIENT["ShopEmpDb"]["ShopsCollection"]
        doc = await shops_collection.find_one({"id": shop_id}, {"additional_infos": 1, "initial_stock_imported": 1, "_id": 0})
        if doc:
            if doc.get("initial_stock_imported") is True:
                return True
            add_infos = doc.get("additional_infos") or {}
            if isinstance(add_infos, dict) and add_infos.get("initial_stock_imported") is True:
                return True
    except Exception as e:
        ic(f"Error checking is_initial_stock_imported via Mongo: {e}")

    # 2. Fallback check via HTTP request to ShopEmp Service
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{SHOPEMP_SERVICE_URL}/shops/by/{shop_id}")
            if resp.status_code == 200:
                data = resp.json()
                shop_data = data.get("data") if isinstance(data, dict) and "data" in data else data
                if isinstance(shop_data, dict):
                    if shop_data.get("initial_stock_imported") is True:
                        return True
                    add_infos = shop_data.get("additional_infos") or shop_data.get("datas") or {}
                    if isinstance(add_infos, dict) and add_infos.get("initial_stock_imported") is True:
                        return True
    except Exception as e:
        ic(f"Error checking is_initial_stock_imported via HTTP: {e}")

    return False
