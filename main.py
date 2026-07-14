from fastapi import FastAPI
from api.routers.v1 import prod_inv_routes,customfield_router
from contextlib import asynccontextmanager
from icecream import ic
from dotenv import load_dotenv
from core.configs.settings_config import SETTINGS
from infras.primary_db.main import init_inventory_pg_db
from hyperlocal_platform.core.enums.environment_enum import EnvironmentEnum
import os,asyncio
from hyperlocal_platform.infras.saga.main import init_infra_db
from infras.primary_db.main import AsyncInventoryLocalSession
from messaging.worker import worker
from infras.read_db.main import DB,INVENTORY_COLLECTION
from infras.caching.main import redis_client,check_redis_health
from background_jobs.cleanup_reservations import cleanup_expired_reservations
from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo
load_dotenv()


@asynccontextmanager
async def inventory_service_lifespan(app:FastAPI):
    try:
        ic("Starting inventory service...")
        await init_infra_db()
        await init_inventory_pg_db()
        await check_redis_health()
        # await redis_client.flushdb()
        # asyncio.create_task(worker())
        # asyncio.create_task(cleanup_expired_reservations())
        app.state.worker_task = asyncio.create_task(worker())
        app.state.cleanup_task = asyncio.create_task(cleanup_expired_reservations())
        yield

    except Exception as e:
        ic(f"Error : Starting inventory service => {e}")

    finally:
        app.state.worker_task.cancel()
        app.state.cleanup_task.cancel()

        await asyncio.gather(
            app.state.worker_task,
            app.state.cleanup_task,
            return_exceptions=True,
        )
        ic("...Stoping inventory Servcie...")

debug=False
openapi_url=None
docs_url=None
redoc_url=None

if SETTINGS.ENVIRONMENT.value==EnvironmentEnum.DEVELOPMENT.value:
    debug=True
    openapi_url="/openapi.json"
    docs_url="/docs"
    redoc_url="/redoc"

app=FastAPI(
    title="Inventory Service",
    description="This service contains all the CRUD operations for Inventory service",
    debug=debug,
    openapi_url=openapi_url,
    docs_url=docs_url,
    redoc_url=redoc_url,
    lifespan=inventory_service_lifespan,
    root_path="/inventories"

)



# Routes to include
app.include_router(prod_inv_routes.router)
app.include_router(customfield_router.router)


