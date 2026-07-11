from fastapi import APIRouter,Depends,Query,HTTPException,UploadFile,File,Form
from schemas.v1.prod_inv_schemas.request_schemas import CreateProdInvSchema,UpdateProdInvSchema,DeleteProdInvSchema
from schemas.v1.inventory_schemas.request_schemas import ReserveInventorySchema,ReleaseInventorySchema,CommitInventorySchema,ReleaseItemInventorySchema
from infras.primary_db.repos.inventory_repo import InventoryRepo
from infras.primary_db.repos.product_repo import ProductRepo
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,BaseResponseTypDict
from ...handlers.prod_inv_handler import HandleProdInvRequest
from typing import Annotated,Optional,List
from infras.primary_db.main import get_pg_async_session,AsyncSession
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from schemas.v1.product_schemas.request_schemas import GetAllProductSchema,GetProductsById,GetProductsByShopId,DeleteProductSchema
from icecream import ic
from pydantic import BaseModel
from integrations.utility_service import upload_assets,delete_assets
from sqlalchemy import select
from infras.primary_db.models.product_model import Products


class UploadImagesSchema(BaseModel):
    shop_id:str
    product_id:str

    @classmethod
    def as_form(
        cls,
        shop_id:str=Form(...),
        product_id:str=Form(...)
    ):
        return cls(
            shop_id=shop_id,
            product_id=product_id
        )


class DeleteImagesSchema(BaseModel):
    shop_id:str
    product_id:str
    urls:List[str]


router=APIRouter(
    tags=["Inventory CRUD's"],
    prefix='/inventories'
)

PG_ASYNC_SESSION=Annotated[AsyncSession,Depends(get_pg_async_session)]

@router.post('')
async def create(data:CreateProdInvSchema,session:PG_ASYNC_SESSION):
    return await HandleProdInvRequest(session=session).create(data=data)


@router.post('/upload/images')
async def upload_images(session:PG_ASYNC_SESSION,data:Annotated[UploadImagesSchema,Depends(UploadImagesSchema.as_form)],files:List[UploadFile]=File(...)):
    stmt_res=(await session.execute(
        select(Products)
        .where(Products.id==data.product_id,Products.shop_id==data.shop_id)
    )).scalar_one_or_none()

    if not stmt_res:
        raise HTTPException(
            status_code=400,
            detail="Product not found"
        )
    
    image_urls = list(stmt_res.image_url) if stmt_res.image_url else []
    if len(image_urls) + len(files) > 3:
        raise HTTPException(
            status_code=400,
            detail="Cannot add more than 3 images in total"
        )
    
    res=await upload_assets(files=files)
    ic(res,data.shop_id,data.product_id)
    
    urls = res.get("data", []) if isinstance(res, dict) else []
    image_urls.extend(urls)
    stmt_res.image_url = image_urls

    await session.commit()

    from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo
    await ProdInvReadDbRepo.add_updatereaddb(shop_id=data.shop_id, product_ids=[data.product_id], session=session)
    return True


@router.delete('/upload/images')
async def delete_images(session:PG_ASYNC_SESSION,data:DeleteImagesSchema):
    stmt_res=(await session.execute(
        select(Products)
        .where(Products.id==data.product_id,Products.shop_id==data.shop_id)
    )).scalar_one_or_none()

    if not stmt_res:
        raise HTTPException(
            status_code=400,
            detail="Product not found"
        )
    
    image_urls = list(stmt_res.image_url) if stmt_res.image_url else []
    FOUND_COUNT=0

    for url in data.urls:
        if url in image_urls:
            FOUND_COUNT+=1
    if len(data.urls)!=FOUND_COUNT:
        raise HTTPException(
            status_code=400,
            detail="Images not Found"
        )
    
    deleted=await delete_assets(urls=data.urls)
    ic(deleted,data.urls,data.shop_id,data.product_id)
    
    final_image_url = [url for url in image_urls if url not in data.urls]
    stmt_res.image_url = final_image_url

    await session.commit()

    from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo
    await ProdInvReadDbRepo.add_updatereaddb(shop_id=data.shop_id, product_ids=[data.product_id], session=session)
    return True


@router.put('')
async def update(data:UpdateProdInvSchema,session:PG_ASYNC_SESSION):
    return await HandleProdInvRequest(session=session).update(data=data)

@router.delete('/{shop_id}/{id}')
async def delete(session:PG_ASYNC_SESSION,data:DeleteProductSchema=Depends()):
    return await HandleProdInvRequest(session=session).delete(data=data)

@router.get('')
async def get_all(session:PG_ASYNC_SESSION,data:GetAllProductSchema=Depends()):
    return await HandleProdInvRequest(session=session).get(data=data)

@router.get('/by/shop/{shop_id}')
async def getby_shop_id(session:PG_ASYNC_SESSION,data:GetProductsByShopId=Depends()):
    return await HandleProdInvRequest(session=session).getby_shop_id(data=data)

@router.get('/by/id/{shop_id}/{id}')
async def getby_id(session:PG_ASYNC_SESSION,data:GetProductsById=Depends()):
    return await HandleProdInvRequest(session=session).getby_id(data=data)

@router.post('/reservations/reserve')
async def reserve_stock(data:ReserveInventorySchema, session:PG_ASYNC_SESSION):
    ic("hello World")
    repo = InventoryRepo(session=session)
    prod_repo=ProductRepo(session=session)
    is_prod_exists=await prod_repo.get_products_by_id(data=GetProductsById(shop_id=data.shop_id,id=data.product_id))
    ic(is_prod_exists)
    if not is_prod_exists:
        raise HTTPException(
            status_code=404,
            detail="Product does not exists"
        )

    has_variant,has_batch,has_serialno=is_prod_exists['type_infos']['has_variant'],is_prod_exists['type_infos']['has_batch'],is_prod_exists['type_infos']['has_serialno']
    if (has_variant and not data.variant_id) or (has_batch and not data.batch_id) and (has_serialno and not data.serialno_infos):
        raise HTTPException(
            status_code=400,
            detail="Some of the variant batch and serialno id was not exists"
        )
    
    res = await repo.reserve_stock(data=data)
    return SuccessResponseTypDict(detail=BaseResponseTypDict(status_code=200, success=res, msg="Stock reserved"))

@router.post('/reservations/release')
async def release_reservations(data:ReleaseInventorySchema, session:PG_ASYNC_SESSION):
    repo = InventoryRepo(session=session)
    res = await repo.release_reservations(data=data)
    return SuccessResponseTypDict(detail=BaseResponseTypDict(status_code=200, success=res, msg="Reservations released"))

@router.post('/reservations/release-item')
async def release_reservation_item(data:ReleaseItemInventorySchema, session:PG_ASYNC_SESSION):
    repo = InventoryRepo(session=session)
    res = await repo.release_reservation_item(data=data)
    return SuccessResponseTypDict(detail=BaseResponseTypDict(status_code=200, success=res, msg="Reservation item released"))

@router.post('/reservations/commit')
async def commit_reservations(data:CommitInventorySchema, session:PG_ASYNC_SESSION):
    repo = InventoryRepo(session=session)
    res = await repo.commit_reservations(data=data)
    return SuccessResponseTypDict(detail=BaseResponseTypDict(status_code=200, success=res, msg="Reservations committed"))
