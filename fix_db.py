import asyncio
import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient

async def fix_purchases():
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client['InventoryServiceDb']
    purchases_col = db['purchase_collections']
    
    purchases = await purchases_col.find({'products.inventory_id': '6c79cd11-c02e-5425-8bf0-b5f2eba6b0a1'}).to_list(None)
    
    conn = await asyncpg.connect('postgresql://postgres:TempSuperSecretPwd@89.167.72.254:5432/InventoryServiceDb')
    rows = await conn.fetch("SELECT id, datas FROM inventory_variants WHERE inventory_id = '6c79cd11-c02e-5425-8bf0-b5f2eba6b0a1'")
    variant_id = rows[0]['id'] if rows else None
    variant_name = 'Color (BLU)'
    for r in rows:
        datas = r['datas']
        import json
        if isinstance(datas, str): datas = json.loads(datas)
        if datas and 'barcode' in datas:
            variant_name = datas['barcode']
            variant_id = r['id']
            break
        
    batch_rows = await conn.fetch("SELECT id, name FROM inventory_batches WHERE inventory_id = '6c79cd11-c02e-5425-8bf0-b5f2eba6b0a1' LIMIT 1")
    batch_id = batch_rows[0]['id'] if batch_rows else None
    batch_name = batch_rows[0]['name'] if batch_rows else None
    
    for p in purchases:
        updated = False
        new_products = []
        for prod in p['products']:
            if prod['inventory_id'] == '6c79cd11-c02e-5425-8bf0-b5f2eba6b0a1':
                if 'variant' not in prod or not prod.get('variant'):
                    prod['variant'] = {
                        'variant_id': variant_id,
                        'variant_name': variant_name
                    }
                    if batch_id:
                        prod['batch'] = {
                            'batch_id': batch_id,
                            'batch_name': batch_name,
                            'mfg_date': '2026-06-12',
                            'exp_date': '2026-06-12'
                        }
                    updated = True
            new_products.append(prod)
            
        if updated:
            await purchases_col.update_one({'_id': p['_id']}, {'$set': {'products': new_products}})
            print(f"Updated purchase {p['purchase_id']}")
            
    await conn.close()

if __name__ == '__main__':
    asyncio.run(fix_purchases())
