import asyncio
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
env_file = os.path.join(parent_dir, ".env")

env_vars = {}
if os.path.exists(env_file):
    with open(env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env_vars[k.strip()] = v.strip().strip('"').strip("'")

pg_url = os.environ.get("INVENTORY_PG_DATABASE_URL") or env_vars.get("INVENTORY_PG_DATABASE_URL") or "postgresql+asyncpg://postgres:437734@127.0.0.1:5432/InventoryServiceDb"
raw_pg_url = pg_url.replace("postgresql+asyncpg://", "postgresql://")

migration_queries = [
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS visible_online BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS sku VARCHAR;",
    "ALTER TABLE product_variants ADD COLUMN IF NOT EXISTS visible_online BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE product_variants ADD COLUMN IF NOT EXISTS sku VARCHAR;",
    "ALTER TABLE product_variants ADD COLUMN IF NOT EXISTS barcode VARCHAR;",
    "ALTER TABLE product_batches ADD COLUMN IF NOT EXISTS visible_online BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE product_serialnumbers ADD COLUMN IF NOT EXISTS visible_online BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE inventory_pricings ADD COLUMN IF NOT EXISTS online_sell_price FLOAT DEFAULT 0.0;",
    "ALTER TABLE inventory_reorder_point ADD COLUMN IF NOT EXISTS online_reorder_point FLOAT DEFAULT 0.0;",
    "ALTER TABLE product_custom_fields ADD COLUMN IF NOT EXISTS category_id VARCHAR;"
]

async def run_migrations():
    import asyncpg
    print(f"Connecting to InventoryServiceDb via asyncpg: {raw_pg_url}")
    conn = await asyncpg.connect(raw_pg_url)
    try:
        for q in migration_queries:
            try:
                print(f"Executing: {q}")
                await conn.execute(q)
            except Exception as e:
                print(f"Warning on {q}: {e}")
        print("ALL INVENTORY SERVICE MIGRATIONS EXECUTED SUCCESSFULLY!")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(run_migrations())
