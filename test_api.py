import urllib.request
import urllib.error
import json
import uuid

# Configuration
BASE_URL = "http://localhost:8004/inventories"
HEADERS = {
    "Content-Type": "application/json",
    "x-user-infos": json.dumps({"id": "test_user", "name": "Test User", "role": "ADMIN"})
}

def make_request(method, url, data=None):
    req = urllib.request.Request(url, method=method, headers=HEADERS)
    if data:
        req.data = json.dumps(data).encode('utf-8')
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        try:
            return e.code, json.loads(body)
        except:
            return e.code, body

def test():
    shop_id = f"shop_{uuid.uuid4().hex[:8]}"
    cat_id = f"cat_{uuid.uuid4().hex[:8]}"
    unit_id = f"unit_{uuid.uuid4().hex[:8]}"
    
    print(f"\n--- Testing Product Creation & Barcode Update ---")
    print(f"Shop ID: {shop_id}")
    
    # 1. Create Product 1
    p1_payload = {
        "shop_id": shop_id,
        "category_id": cat_id,
        "unit_id": unit_id,
        "name": "Test Product 1",
        "description": "Description 1",
        "barcode": "BARCODE-P1",
        "type_infos": {
            "has_variant": False,
            "has_batch": False,
            "has_serialno": False
        },
        "have_tracking": False,
        "buy_price": 10.0,
        "sell_price": 20.0
    }
    
    status, p1_res = make_request("POST", BASE_URL, p1_payload)
    print(f"\nCreate Product 1 Response: {status}")
    if status != 201:
        print(p1_res)
        return
    p1_data = p1_res.get("data", {})
    p1_id = p1_data.get("id")
    print(f"✅ Product 1 Created with ID: {p1_id} and Barcode: BARCODE-P1")

    # 2. Create Product 2
    p2_payload = {
        "shop_id": shop_id,
        "category_id": cat_id,
        "unit_id": unit_id,
        "name": "Test Product 2",
        "description": "Description 2",
        "barcode": "BARCODE-P2",
        "type_infos": {
            "has_variant": False,
            "has_batch": False,
            "has_serialno": False
        },
        "have_tracking": False,
        "buy_price": 15.0,
        "sell_price": 25.0
    }
    
    status, p2_res = make_request("POST", BASE_URL, p2_payload)
    print(f"\nCreate Product 2 Response: {status}")
    if status != 201:
        print(p2_res)
        return
    p2_data = p2_res.get("data", {})
    p2_id = p2_data.get("id")
    print(f"✅ Product 2 Created with ID: {p2_id} and Barcode: BARCODE-P2")

    # 3. Update Product 1 barcode to BARCODE-P2 (should fail)
    upd1_payload = {
        "id": p1_id,
        "shop_id": shop_id,
        "barcode": "BARCODE-P2"
    }
    status, upd1_res = make_request("PUT", BASE_URL, upd1_payload)
    print(f"\nUpdate Product 1 (Duplicate Barcode) Response: {status}")
    if status == 400:
        detail = upd1_res.get("detail") if isinstance(upd1_res, dict) else upd1_res
        print(f"✅ PASSED: Successfully caught duplicate barcode validation error: {detail}")
    else:
        print(f"❌ FAILED: Expected 400 error but got {status}. Response: {upd1_res}")

    # 4. Update Product 1 barcode to a new valid barcode (BARCODE-P3)
    upd2_payload = {
        "id": p1_id,
        "shop_id": shop_id,
        "barcode": "BARCODE-P3"
    }
    status, upd2_res = make_request("PUT", BASE_URL, upd2_payload)
    print(f"\nUpdate Product 1 (Valid Barcode) Response: {status}")
    if status == 200:
        print(f"✅ PASSED: Successfully updated Product 1 to a new valid barcode!")
    else:
        print(f"❌ FAILED: Expected 200 success but got {status}. Response: {upd2_res}")

if __name__ == "__main__":
    test()
