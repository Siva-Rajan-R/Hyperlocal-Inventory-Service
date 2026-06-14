import asyncio
from sqlalchemy import select, update, delete
from infras.primary_db.main import AsyncInventoryLocalSession
from infras.primary_db.models.inventory_model import InventorySerialNumbers

async def merge_serials():
    async with AsyncInventoryLocalSession() as session:
        # Get all serial numbers
        stmt = select(InventorySerialNumbers)
        rows = (await session.execute(stmt)).scalars().all()
        
        # Group by unique key
        groups = {}
        for row in rows:
            key = (row.shop_id, row.inventory_id, row.variant_id, row.batch_id)
            groups.setdefault(key, []).append(row)
            
        for key, group in groups.items():
            if len(group) > 1:
                print(f"Merging {len(group)} rows for key {key}")
                # Keep the first row
                primary_row = group[0]
                # Merge all serial numbers
                all_serials = []
                for r in group:
                    if r.serial_numbers:
                        all_serials.extend(r.serial_numbers)
                
                # Update primary row
                primary_row.serial_numbers = list(set(all_serials)) # or just all_serials if duplicates are fine
                
                # Delete other rows
                for r in group[1:]:
                    await session.delete(r)
                    
        await session.commit()
        print("Done merging!")

if __name__ == "__main__":
    asyncio.run(merge_serials())
