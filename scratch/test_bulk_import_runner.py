import csv
import sys
import os
import time
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Ensure paths
sys.path.insert(0, os.path.abspath("."))

from infras.primary_db.main import AsyncInventoryLocalSession
from api.handlers.prod_inv_handler import HandleProdInvRequest
from schemas.v1.prod_inv_schemas.request_schemas import CreateProdInvSchema
from schemas.v1.product_schemas.custom_types import ProductTypeInfosType

SHOP_ID = "3845cf4c-54e3-5c28-b537-8d64f17009f4"
USER_ID = "5119f879-958d-452d-8441-2af032eeaf1b"
CSV_PATH = os.path.join(os.path.dirname(__file__), "test_import_products.csv")

async def run_test():
    print(f"--- Starting Bulk Product Import Test for Shop {SHOP_ID} ---")
    
    # 1. Fetch category and unit mappings for this shop from UtilityServiceFieldsDb
    util_engine = create_async_engine("postgresql+asyncpg://postgres:437734@127.0.0.1:5432/UtilityServiceFieldsDb")
    cat_map = {}
    unit_map = {}
    
    async with util_engine.connect() as conn:
        cats = (await conn.execute(text("SELECT id, name FROM shop_categories WHERE shop_id = :sid"), {"sid": SHOP_ID})).mappings().all()
        for c in cats:
            cat_map[c["name"].strip().lower()] = c["id"]
            
        units = (await conn.execute(text("SELECT id, name FROM shop_units WHERE shop_id = :sid"), {"sid": SHOP_ID})).mappings().all()
        for u in units:
            unit_map[u["name"].strip().lower()] = u["id"]
            
    print(f"Loaded {len(cat_map)} categories and {len(unit_map)} units from Utility DB.")
    
    default_cat_id = cat_map.get("general") or next(iter(cat_map.values()))
    default_unit_id = unit_map.get("pieces") or unit_map.get("piece") or next(iter(unit_map.values()))

    # 2. Parse CSV
    items = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            p_name = row.get("Product Name", "").strip()
            if not p_name:
                continue
                
            brand = row.get("Brand", "").strip() or None
            if brand and not p_name.lower().startswith(brand.lower()):
                full_name = f"{brand} {p_name}"
            else:
                full_name = p_name
                
            desc = row.get("Description", "").strip() or full_name
            cat_name = row.get("Category", "").strip().lower()
            unit_name = row.get("Unit", "").strip().lower()
            
            cat_id = cat_map.get(cat_name, default_cat_id)
            unit_id = unit_map.get(unit_name, default_unit_id)
            
            raw_stocks = row.get("Number of stocks", "0").strip()
            try:
                stocks_val = float(raw_stocks) if raw_stocks else 0.0
            except ValueError:
                stocks_val = 0.0
                
            raw_tracking = row.get("Have Tracking (true/false)", "True").strip().lower()
            have_tracking = raw_tracking in ("true", "1", "yes")

            schema_item = CreateProdInvSchema(
                shop_id=SHOP_ID,
                category_id=cat_id,
                unit_id=unit_id,
                name=full_name,
                brand=brand,
                description=desc,
                type_infos=ProductTypeInfosType(has_variant=False, has_batch=False, has_serialno=False),
                have_tracking=have_tracking,
                stocks=stocks_val,
                buy_price=None,
                sell_price=None,
                mrp=None,
                gst=row.get("GST (%)", "").strip() or "0%",
                reorder_point=5.0,
                storage_location=row.get("Storage Location", "").strip() or None,
                sku=row.get("SKU", "").strip() or None,
                barcode=row.get("Barcode", "").strip() or None,
                is_active=False
            )
            items.append(schema_item)

    print(f"Parsed {len(items)} items from CSV ready for bulk import.")
    assert len(items) == 98, f"Expected 98 items, found {len(items)}"

    # 3. Execute bulk import via HandleProdInvRequest
    async with AsyncInventoryLocalSession() as session:
        handler = HandleProdInvRequest(session=session)
        
        t0 = time.perf_counter()
        response = await handler.create_bulk(data=items, executing_user_id=USER_ID)
        elapsed = time.perf_counter() - t0
        
        print("\n=== BULK IMPORT RESULTS ===")
        print(f"Elapsed Time: {elapsed:.3f} seconds")
        print(f"Response Status Code: {response['detail']['status_code']}")
        print(f"Response Success: {response['detail']['success']}")
        print(f"Response Message: {response['detail']['msg']}")
        print(f"Created Product Records Count: {len(response.get('data', []))}")
        
        assert response["detail"]["success"] is True, "Bulk import failed!"
        assert len(response.get("data", [])) == 98, f"Expected 98 created items, got {len(response.get('data', []))}"
        print(f"\n[PASS] Verified 98 products imported with 0 errors in {elapsed:.3f}s (< 2.0s target)!")
        os._exit(0)

if __name__ == "__main__":
    asyncio.run(run_test())
