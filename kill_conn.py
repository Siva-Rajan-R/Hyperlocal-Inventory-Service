import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def kill_idle():
    engine = create_async_engine("postgresql+asyncpg://postgres:TempSuperSecretPwd@89.167.72.254:5432/postgres")
    try:
        async with engine.connect() as conn:
            # Terminate all connections to InventoryServiceDb except this one
            await conn.execute(text("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = 'InventoryServiceDb'
                AND pid <> pg_backend_pid()
                AND state = 'idle';
            """))
            await conn.commit()
            print("Successfully killed idle connections.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await engine.dispose()

asyncio.run(kill_idle())
