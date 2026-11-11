from sqlalchemy import func
from hyperlocal_platform.core.enums.timezone_enum import TimeZoneEnum
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.exceptions import HTTPException
from schemas.v1.prod_inv_schemas.request_schemas import CreateProdInvSchema,UpdateProdInvSchema,DeleteProdInvSchema
from hyperlocal_platform.core.models.req_res_models import SuccessResponseTypDict,BaseResponseTypDict,ErrorResponseTypDict
from messaging.saga_producer import SagaProducer,CreateSagaStateSchema,SagaStatusEnum
from hyperlocal_platform.core.enums.saga_state_enum import SagaStepsValueEnum
from hyperlocal_platform.core.typed_dicts.saga_status_typ_dict import SagaStateExecutionTypDict
from hyperlocal_platform.core.enums.routingkey_enum import RoutingkeyActions,RoutingkeyState,RoutingkeyVersions
from hyperlocal_platform.core.utils.routingkey_builder import generate_routingkey
from hyperlocal_platform.core.utils.uuid_generator import generate_uuid
from core.constants import SERVICE_NAME
from infras.primary_db.services.prod_inv_service import ProductInventoryService
from schemas.v1.product_schemas.request_schemas import GetAllProductSchema,GetProductsById,GetProductsByShopId,GetBulkProductsById
from infras.primary_db.repos.product_repo import ProductRepo
from core.data_formats.enums.inventory_enums import InventoryFetchMode
from core.utils.calculate_offer import calculate_offer
from core.utils.validate_offer import validate_offer_input
from icecream import ic
from infras.read_db.repos.prod_inv_repo import ProdInvReadDbRepo
from infras.primary_db.services.customfield_service import CustomFieldsService,GetFieldByShopIdSchema
from schemas.v1.prod_inv_schemas.request_schemas import CreateProdInvSchema,UpdateProdInvSchema,DeleteProdInvSchema
from schemas.v1.inventory_schemas.request_schemas import ReserveInventorySchema,ReleaseInventorySchema,CommitInventorySchema,ReleaseItemInventorySchema
from core.utils.validate_custom_fields import validate_and_filter_custom_fields
from schemas.v1.request_schemas.customfield_schema import BulkCreateCustomFieldValuesSchema
# [InventoryGetResponseSchema(**r) for r in res] if res else []
import os

def _extract_positive_float(val) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        num = float(val)
        return num if num > 0 else None
    except (TypeError, ValueError):
        return None


def _raise_mrp_error(description: str):
    raise HTTPException(
        status_code=400,
        detail=ErrorResponseTypDict(
            msg="Invalid Pricing",
            description=description,
            success=False,
            status_code=400,
        ),
    )


class HandleProdInvRequest:
    def __init__(self,session:AsyncSession):
        self.session=session

    def _validate_pricing_against_mrp(
        self,
        data,
        fallback_mrp: Optional[float] = None,
        fallback_sell_price: Optional[float] = None,
        fallback_online_sell_price: Optional[float] = None,
        variant_db_map: Optional[dict] = None,
        product_label: Optional[str] = None,
    ):
        fields_set = getattr(data, "model_fields_set", set())
        mrp_explicitly_set = "mrp" in fields_set
        raw_cf_mrp = (data.custom_fields or {}).get("mrp") if isinstance(getattr(data, "custom_fields", None), dict) else None

        if mrp_explicitly_set:
            if data.mrp is None or (isinstance(data.mrp, (int, float)) and float(data.mrp) <= 0):
                effective_mrp = _extract_positive_float(raw_cf_mrp)
            else:
                effective_mrp = _extract_positive_float(data.mrp)
        else:
            effective_mrp = _extract_positive_float(getattr(data, "mrp", None)) or _extract_positive_float(raw_cf_mrp) or _extract_positive_float(fallback_mrp)

        suffix = f" for product '{product_label}'" if product_label else ""
        check_sp = data.sell_price if getattr(data, "sell_price", None) is not None else fallback_sell_price
        check_osp = data.online_sell_price if getattr(data, "online_sell_price", None) is not None else fallback_online_sell_price

        if effective_mrp is not None and effective_mrp > 0:
            if check_sp is not None and float(check_sp) > effective_mrp:
                _raise_mrp_error(f"Selling price ({check_sp}) cannot exceed MRP ({effective_mrp}){suffix}.")
            if check_osp is not None and float(check_osp) > effective_mrp:
                _raise_mrp_error(f"Online selling price ({check_osp}) cannot exceed MRP ({effective_mrp}){suffix}.")

        variant_infos = getattr(data, "variant_infos", None) or []
        v_map = variant_db_map or {}
        for variant in variant_infos:
            v_fields_set = getattr(variant, "model_fields_set", set())
            v_id = getattr(variant, "id", None)
            v_db = v_map.get(v_id, {}) if v_id else {}
            if "mrp" in v_fields_set and (variant.mrp is None or float(variant.mrp) <= 0):
                v_mrp = effective_mrp
            else:
                v_mrp = _extract_positive_float(getattr(variant, "mrp", None)) or _extract_positive_float(v_db.get("mrp")) or effective_mrp

            v_sp = variant.sell_price if getattr(variant, "sell_price", None) is not None else v_db.get("sell_price")
            v_osp = variant.online_sell_price if getattr(variant, "online_sell_price", None) is not None else v_db.get("online_sell_price")
            v_label = getattr(variant, "name", None) or v_id or "variant"

            if v_mrp is not None and v_mrp > 0:
                if v_sp is not None and float(v_sp) > v_mrp:
                    _raise_mrp_error(f"Variant '{v_label}' selling price ({v_sp}) cannot exceed MRP ({v_mrp}).")
                if v_osp is not None and float(v_osp) > v_mrp:
                    _raise_mrp_error(f"Variant '{v_label}' online selling price ({v_osp}) cannot exceed MRP ({v_mrp}).")
    
    async def create(self,data:CreateProdInvSchema, executing_user_id: Optional[str] = None):
        """
        Instead of creation, we need to trigger the event that event will handle the adding
        """
        # Check Subscription status and SKU limit
        from integrations.shop_service import get_shop_subscription
        sub_info = await get_shop_subscription(data.shop_id)

        # Check subscription environment: in development, creation is unlimited
        sub_env = (os.getenv("SUBSCRIPTION_ENVIRONMENT") or os.getenv("ENVIRONMENT") or "development").strip().lower()
        is_dev = sub_env in ("development", "dev")

        if not is_dev:
            if sub_info.get("status") == "expired" or sub_info.get("is_expired") is True:
                raise HTTPException(
                    status_code=403,
                    detail=ErrorResponseTypDict(
                        msg="Subscription Expired",
                        description="Your subscription has expired. Creating products is paused until renewed.",
                        success=False,
                        status_code=403
                    )
                )

            max_skus = sub_info.get("limits", {}).get("max_skus", 500)
            
            # Calculate live SKU count using fast MongoDB aggregation
            current_skus = 0
            try:
                from infras.read_db.main import MONGO_CLIENT
                pipeline = [
                    {"$match": {"shop_id": data.shop_id}},
                    {
                        "$project": {
                            "sku_count": {
                                "$cond": {
                                    "if": {
                                        "$and": [
                                            {"$ne": ["$variants", None]},
                                            {"$gt": [{"$size": {"$ifNull": [{"$objectToArray": "$variants"}, []]}}, 0]}
                                        ]
                                    },
                                    "then": {"$size": {"$objectToArray": "$variants"}},
                                    "else": 1
                                }
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total_skus": {"$sum": "$sku_count"}
                        }
                    }
                ]
                agg_res = await MONGO_CLIENT["InventoryServiceReadDb"]["ProdInvCollections"].aggregate(pipeline).to_list(1)
                if agg_res:
                    current_skus = agg_res[0].get("total_skus", 0)
            except Exception:
                # Fallback to Primary DB count
                from infras.primary_db.models.product_model import Products, Variants
                prod_count = (await self.session.execute(select(func.count(Products.id)).where(Products.shop_id == data.shop_id))).scalar() or 0
                var_count = (await self.session.execute(select(func.count(Variants.id)).where(Variants.shop_id == data.shop_id))).scalar() or 0
                var_prod_count = (await self.session.execute(select(func.count(Products.id)).where(Products.shop_id == data.shop_id, Products.type_infos.op("->>")("has_variant") == "true"))).scalar() or 0
                current_skus = max(0, prod_count - var_prod_count) + var_count

            new_skus = len(data.variant_infos) if (data.type_infos and data.type_infos.has_variant and data.variant_infos) else 1
            if current_skus + new_skus > max_skus:
                raise HTTPException(
                    status_code=403,
                    detail=ErrorResponseTypDict(
                        msg="SKU Limit Reached",
                        description=f"Catalogue limit of {max_skus} SKUs reached ({current_skus}/{max_skus}). Upgrade your plan or add SKU expansion packs.",
                        success=False,
                        status_code=403
                    )
                ) 
        if data.type_infos.has_variant and not data.variant_infos:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error => Creating Inventory",
                    description="Please provide atleast one variant when it was enabled",
                    success=False,
                    status_code=400
                ),
                
            )
        
        # Check for similar product names
        repo = ProductRepo(session=self.session)
        existing_names = await repo.get_product_names_by_shop_id(shop_id=data.shop_id)
        from core.utils.similarity_check import find_similar_name
        similar_name = find_similar_name(data.name, existing_names)
        
        if similar_name:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Similarity Check Failed",
                    description=f"A product with a similar name '{similar_name}' already exists. Please choose a different name.",
                    success=False,
                    status_code=400
                )
            )

        
        # Validate cost to make (buy price) for untracked / off-the-track products
        if data.have_tracking is False:
            if data.type_infos and data.type_infos.has_variant and data.variant_infos:
                for variant in data.variant_infos:
                    if variant.buy_price is None or float(variant.buy_price) <= 0:
                        raise HTTPException(
                            status_code=400,
                            detail=ErrorResponseTypDict(
                                msg="Validation Error",
                                description=f"Cost to make (buy price) is mandatory for variant '{variant.name or 'variant'}' of untracked products.",
                                success=False,
                                status_code=400
                            )
                        )
            else:
                if data.buy_price is None or float(data.buy_price) <= 0:
                    raise HTTPException(
                        status_code=400,
                        detail=ErrorResponseTypDict(
                            msg="Validation Error",
                            description="Cost to make (buy price) is mandatory for untracked products.",
                            success=False,
                            status_code=400
                        )
                    )
        # Validate selling price vs MRP (applies to both tracked and untracked products)
        self._validate_pricing_against_mrp(data)

        # Check if opening stock is provided and if initial stock import is already completed
        has_stock = False
        if data.type_infos and data.type_infos.has_variant and data.variant_infos:
            has_stock = any(v.stocks is not None and float(v.stocks) > 0 for v in data.variant_infos)
        elif data.stocks is not None and float(data.stocks) > 0:
            has_stock = True

        if has_stock:
            from integrations.shop_service import is_initial_stock_imported
            if await is_initial_stock_imported(data.shop_id):
                raise HTTPException(
                    status_code=400,
                    detail=ErrorResponseTypDict(
                        msg="Initial Stock Import Completed",
                        description="Initial stock import has already been completed for this shop. Products cannot be created with opening stock. Please add stock via Purchases or Stock Adjustments.",
                        success=False,
                        status_code=400
                    )
                )
        
        cust_field_obj=CustomFieldsService(session=self.session)
        fields=await cust_field_obj.get_field_by_shop_id(data=GetFieldByShopIdSchema(shop_id=data.shop_id))
        
        raw_cf = dict(data.custom_fields) if data.custom_fields and isinstance(data.custom_fields, dict) else {}
        valid_custom_fields = validate_and_filter_custom_fields(data.custom_fields, fields, category_id=data.category_id)
        merged_custom_fields = {**raw_cf, **valid_custom_fields}
        
        res=await ProductInventoryService(session=self.session).create(data=CreateProdInvSchema(custom_fields=merged_custom_fields,**data.model_dump(exclude=['custom_fields'])), executing_user_id=executing_user_id)
        ic(res)
        
        if res:
            await self.session.commit()
            return SuccessResponseTypDict(
                detail=BaseResponseTypDict(
                    status_code=201,
                    success=True,
                    msg="Inventory Created Successfully"
                ),
                data=res
            )
        
        return HTTPException(
             status_code=400,
             detail=ErrorResponseTypDict(
                  msg="Error => Creating Inventory",
                  description="Invalid payload for creating invetory product or already exists",
                  success=False,
                  status_code=400
             ),
             
        )

    async def create_bulk(self, data: List[CreateProdInvSchema], executing_user_id: Optional[str] = None):
        # Check if any item has opening stocks and shop has completed initial stock import
        shops_to_check = set(item.shop_id for item in data)
        from integrations.shop_service import is_initial_stock_imported
        for shop_id in shops_to_check:
            shop_items = [item for item in data if item.shop_id == shop_id]
            has_opening_stock = False
            for item in shop_items:
                if item.type_infos and item.type_infos.has_variant and item.variant_infos:
                    if any(v.stocks is not None and float(v.stocks) > 0 for v in item.variant_infos):
                        has_opening_stock = True
                        break
                elif item.stocks is not None and float(item.stocks) > 0:
                    has_opening_stock = True
                    break

            if has_opening_stock and await is_initial_stock_imported(shop_id):
                raise HTTPException(
                    status_code=400,
                    detail=ErrorResponseTypDict(
                        msg="Initial Stock Import Completed",
                        description="Initial stock import has already been completed for this shop. Products cannot be imported or created with opening stock directly. Please add stock via Purchases or Stock Adjustments.",
                        success=False,
                        status_code=400
                    )
                )

        # Validate selling price vs MRP in bulk items (both tracked and untracked)
        for item in data:
            self._validate_pricing_against_mrp(item, product_label=item.name)

        valid_items = []
        cust_field_obj = CustomFieldsService(session=self.session)
        for item in data:
            if item.type_infos.has_variant and not item.variant_infos:
                continue

            fields = await cust_field_obj.get_field_by_shop_id(data=GetFieldByShopIdSchema(shop_id=item.shop_id))
            raw_cf = dict(item.custom_fields) if item.custom_fields and isinstance(item.custom_fields, dict) else {}
            valid_custom_fields = validate_and_filter_custom_fields(item.custom_fields, fields, category_id=item.category_id)
            item_data = item.model_dump()
            item_data["custom_fields"] = {**raw_cf, **valid_custom_fields}
            valid_items.append(CreateProdInvSchema(**item_data))
            
        res = await ProductInventoryService(session=self.session).create_bulk_prodinv_items(data=valid_items, executing_user_id=executing_user_id)
        if res:
            await self.session.commit()
            return SuccessResponseTypDict(
                detail=BaseResponseTypDict(
                    status_code=201,
                    success=True,
                    msg="Bulk Inventory Products Created Successfully"
                ),
                data=res
            )
        raise HTTPException(
            status_code=400,
            detail=ErrorResponseTypDict(
                msg="Error => Creating Bulk Inventory Products",
                description="Failed to create products in bulk",
                success=False,
                status_code=400
            )
        )
    
    async def update(self,data:UpdateProdInvSchema, executing_user_id: Optional[str] = None):
        if data.type_infos and data.type_infos.has_variant and not data.variant_infos:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error => Updating Inventory",
                    description="Please provide atleast one variant when it was enabled",
                    success=False,
                    status_code=400
                ),
            )
        # Validate selling price vs MRP on update (for both tracked and untracked products)
        db_mrp = None
        db_sell_price = None
        db_online_sell_price = None
        variant_db_map = {}
        try:
            from infras.primary_db.models.inventory_model import InventoryPricings
            from infras.primary_db.models.product_model import Products
            from sqlalchemy import select

            p_stmt = select(
                InventoryPricings.variant_id,
                InventoryPricings.sell_price,
                InventoryPricings.online_sell_price,
                InventoryPricings.additional_infos,
            ).where(
                InventoryPricings.product_id == data.id,
                InventoryPricings.shop_id == data.shop_id,
                InventoryPricings.is_active == True,
            )
            pricing_rows = (await self.session.execute(p_stmt)).all()
            for v_id, row_sp, row_osp, row_add in pricing_rows:
                add_mrp = row_add.get("mrp") if isinstance(row_add, dict) else None
                if v_id:
                    entry = variant_db_map.setdefault(v_id, {})
                    if entry.get("mrp") is None and add_mrp is not None:
                        entry["mrp"] = add_mrp
                    if entry.get("sell_price") is None and row_sp is not None:
                        entry["sell_price"] = row_sp
                    if entry.get("online_sell_price") is None and row_osp is not None:
                        entry["online_sell_price"] = row_osp
                else:
                    if db_mrp is None and add_mrp is not None:
                        db_mrp = add_mrp
                    if db_sell_price is None and row_sp is not None:
                        db_sell_price = row_sp
                    if db_online_sell_price is None and row_osp is not None:
                        db_online_sell_price = row_osp

            cf_stmt = select(Products.custom_fields).where(Products.id == data.id, Products.shop_id == data.shop_id)
            existing_cf = (await self.session.execute(cf_stmt)).scalar_one_or_none()
            if isinstance(existing_cf, dict):
                if db_mrp is None:
                    db_mrp = existing_cf.get("mrp")
                if db_sell_price is None:
                    db_sell_price = existing_cf.get("sell_price")
                if db_online_sell_price is None:
                    db_online_sell_price = existing_cf.get("online_sell_price")
        except Exception:
            pass

        self._validate_pricing_against_mrp(
            data,
            fallback_mrp=db_mrp,
            fallback_sell_price=db_sell_price,
            fallback_online_sell_price=db_online_sell_price,
            variant_db_map=variant_db_map,
        )
        valid_custom_fields = None
        raw_cf = dict(data.custom_fields) if data.custom_fields and isinstance(data.custom_fields, dict) else None
        if data.custom_fields is not None:
            cust_field_obj = CustomFieldsService(session=self.session)
            fields = await cust_field_obj.get_field_by_shop_id(data=GetFieldByShopIdSchema(shop_id=data.shop_id))
            cat_id = data.category_id
            if not cat_id:
                from infras.primary_db.models.product_model import Products
                from sqlalchemy import select
                cat_stmt = select(Products.category_id).where(Products.id == data.id, Products.shop_id == data.shop_id)
                cat_id = (await self.session.execute(cat_stmt)).scalar_one_or_none()
            valid_custom_fields = validate_and_filter_custom_fields(data.custom_fields, fields, category_id=cat_id)
            if valid_custom_fields and raw_cf is not None:
                if "values" not in raw_cf or not raw_cf.get("values"):
                    raw_cf["values"] = [{"field_id": fid, "value": fval} for fid, fval in valid_custom_fields.items()]
        
        dumped_data = data.model_dump(exclude_unset=True)
        if raw_cf is not None:
            dumped_data["custom_fields"] = {**raw_cf, **(valid_custom_fields or {})}
        elif "custom_fields" in dumped_data and data.custom_fields is None:
            dumped_data.pop("custom_fields", None)

        res = await ProductInventoryService(session=self.session).update(
            data=UpdateProdInvSchema(**dumped_data),
            executing_user_id=executing_user_id
        )
        ic(res)
        if res:
             await self.session.commit()
             return SuccessResponseTypDict(
                  detail=BaseResponseTypDict(
                       status_code=200,
                       success=True,
                       msg="Inventory Updated Successfully"
                  )
             )
        
        raise HTTPException(
             status_code=400,
             detail=ErrorResponseTypDict(
                  msg="Error => Updating Inventory",
                  description="Invalid payload for updating invetory product",
                  success=False,
                  status_code=400
             ),
         )
    
    async def delete(self,data:DeleteProdInvSchema, executing_user_id: Optional[str] = None):
        try:
            res=await ProductInventoryService(session=self.session).delete(data=data, executing_user_id=executing_user_id)
            ic(res)
            if res:
                return SuccessResponseTypDict(
                    detail=BaseResponseTypDict(
                        status_code=200,
                        success=True,
                        msg="Inventory Deleted Successfully"
                    )
                )
            
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error => Deleting Inventory",
                    description="Invalid payload for deleting inventory product or product not found",
                    success=False,
                    status_code=400
                ),
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=ErrorResponseTypDict(
                    msg="Error => Deleting Inventory",
                    description=str(e),
                    success=False,
                    status_code=400
                )
            )
    
    async def get(self, data: GetAllProductSchema):
        try:
            res = await ProdInvReadDbRepo.get_all(data=data)
        except Exception as e:
            ic(f"Error in read db get: {e}")
            res = None

        if not res and self.session:
            res = await ProductRepo(session=self.session).get_products(data=data)

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventories fetched successfully",
                status_code=200,
                success=True
            ),
            data=res or []
        )

    async def getby_shop_id(self, data: GetProductsByShopId):
        try:
            res = await ProdInvReadDbRepo.get_by_shop_id(data=data)
        except Exception as e:
            ic(f"Error in read db get_by_shop_id: {e}")
            res = None

        if not res and self.session:
            res = await ProductRepo(session=self.session).get_products_by_shop_id(data=data)

        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventories fetched successfully",
                status_code=200,
                success=True
            ),
            data=res or []
        )
    
    async def getby_id(self,data:GetProductsById):
        # res=await ProductRepo(session=self.session).get_products_by_id(data=data)
        res=await ProdInvReadDbRepo.get_by_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventories fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )

    async def get_bulk_by_ids(self, data: GetBulkProductsById):
        res = await ProdInvReadDbRepo.get_bulk_by_id(data=data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventories fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )
        
    async def search(self, shop_id: str, query: str, limit: int = 5):
        from infras.read_db.repos.inventory_repo import InventoryReadDbRepo
        from schemas.v1.request_schemas.inventory_schema import GetInventoryByShopIdSchema
        req_data = GetInventoryByShopIdSchema(shop_id=shop_id, query=query, limit=limit, offset=1)
        res = await InventoryReadDbRepo.get_all_inventories(data=req_data)
        return SuccessResponseTypDict(
            detail=BaseResponseTypDict(
                msg="Inventory fetched successfully",
                status_code=200,
                success=True
            ),
            data=res
        )