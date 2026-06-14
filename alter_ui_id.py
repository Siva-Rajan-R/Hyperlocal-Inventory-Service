import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://postgres:TempSuperSecretPwd@89.167.72.254:5432/InventoryServiceDb')
    
    # Inventory
    await conn.execute('ALTER TABLE inventory ALTER COLUMN ui_id DROP IDENTITY IF EXISTS')
    await conn.execute('ALTER TABLE inventory ALTER COLUMN ui_id TYPE VARCHAR USING ui_id::VARCHAR')
    
    # Purchase
    await conn.execute('ALTER TABLE purchase ALTER COLUMN ui_id DROP IDENTITY IF EXISTS')
    await conn.execute('ALTER TABLE purchase ALTER COLUMN ui_id TYPE VARCHAR USING ui_id::VARCHAR')
    
    # Stock Adjustments
    await conn.execute('ALTER TABLE stock_adjustments ALTER COLUMN ui_id DROP IDENTITY IF EXISTS')
    await conn.execute('ALTER TABLE stock_adjustments ALTER COLUMN ui_id TYPE VARCHAR USING ui_id::VARCHAR')
    
    print('Altered inventory, purchase, stock_adjustments tables!')
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
