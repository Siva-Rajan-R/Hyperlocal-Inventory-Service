import asyncio
import uuid
from infras.primary_db.main import AsyncInventoryLocalSession
from infras.primary_db.services.prod_inv_service import ProductInventoryService
from schemas.v1.prod_inv_schemas.request_schemas import CreateProdInvSchema, UpdateProdInvSchema
from core.data_formats.custom_types.product_custom_types import ProductTypeInfosType

async def test_barcode_update():
    session = AsyncInventoryLocalSession()
    shop_id = "test-shop-barcode-123"
    cat_id = "test-cat-123"
    unit_id = "test-unit-123"

    try:
        service = ProductInventoryService(session=session)
        
        print("\n--- Test 1: Create Product 1 with barcode B1 ---")
        p1_data = CreateProdInvSchema(
            shop_id=shop_id,
            category_id=cat_id,
            unit_id=unit_id,
            name="Product 1",
            description="Test Prod 1",
            barcode="BARCODE-B1",
            type_infos=ProductTypeInfosType(
                has_variant=False,
                has_batch=False,
                has_serialno=False
            ),
            have_tracking=False
        )
        p1_res = await service.create(data=p1_data, executing_user_id="test_user")
        p1_id = p1_res["id"]
        print(f"Product 1 created: {p1_id} with barcode BARCODE-B1")

        print("\n--- Test 2: Create Product 2 with barcode B2 ---")
        p2_data = CreateProdInvSchema(
            shop_id=shop_id,
            category_id=cat_id,
            unit_id=unit_id,
            name="Product 2",
            description="Test Prod 2",
            barcode="BARCODE-B2",
            type_infos=ProductTypeInfosType(
                has_variant=False,
                has_batch=False,
                has_serialno=False
            ),
            have_tracking=False
        )
        p2_res = await service.create(data=p2_data, executing_user_id="test_user")
        p2_id = p2_res["id"]
        print(f"Product 2 created: {p2_id} with barcode BARCODE-B2")

        print("\n--- Test 3: Attempt to update Product 1 to have barcode B2 (Should Fail) ---")
        upd1_data = UpdateProdInvSchema(
            id=p1_id,
            shop_id=shop_id,
            barcode="BARCODE-B2"
        )
        try:
            await service.update(data=upd1_data, executing_user_id="test_user")
            print("❌ Test 3 FAILED: Expected ValueError for duplicate barcode but got success.")
        except ValueError as e:
            print(f"✅ Test 3 PASSED: Caught expected ValueError: {e}")

        print("\n--- Test 4: Attempt to update Product 1 to have barcode B3 (Should Succeed) ---")
        upd2_data = UpdateProdInvSchema(
            id=p1_id,
            shop_id=shop_id,
            barcode="BARCODE-B3"
        )
        await service.update(data=upd2_data, executing_user_id="test_user")
        print("✅ Test 4 PASSED: Successfully updated Product 1 to BARCODE-B3")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await session.close()
        
if __name__ == "__main__":
    asyncio.run(test_barcode_update())
