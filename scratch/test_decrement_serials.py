import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from icecream import ic

def test_decrement_serial_resolution():
    # Mock prod_db for 4 cases
    prod_db_base = {
        "id": "p1",
        "type_infos": {"has_variant": False, "has_batch": False, "has_serialno": True},
        "serialno_infos": [{"id": "s1_uuid", "name": "SN-BASE-001"}]
    }

    prod_db_variant = {
        "id": "p2",
        "type_infos": {"has_variant": True, "has_batch": False, "has_serialno": True},
        "variants": {
            "v1": {
                "id": "v1",
                "serialno_infos": [{"id": "s2_uuid", "name": "SN-VAR-001"}]
            }
        }
    }

    prod_db_batch = {
        "id": "p3",
        "type_infos": {"has_variant": False, "has_batch": True, "has_serialno": True},
        "batch_infos": [
            {
                "id": "b1",
                "name": "BATCH-100",
                "serialno_infos": [{"id": "s3_uuid", "name": "SN-BATCH-001"}]
            }
        ]
    }

    prod_db_var_batch = {
        "id": "p4",
        "type_infos": {"has_variant": True, "has_batch": True, "has_serialno": True},
        "variants": {
            "v2": {
                "id": "v2",
                "batch_infos": [
                    {
                        "id": "b2",
                        "name": "BATCH-200",
                        "serialno_infos": [{"id": "s4_uuid", "name": "SN-VARBATCH-001"}]
                    }
                ]
            }
        }
    }

    def resolve_serials(prod_db, inc_variant_id, inc_batch_infos, inc_serialnos):
        has_variant = prod_db['type_infos']['has_variant']
        has_batch = prod_db['type_infos']['has_batch']

        inc_batch_id = (inc_batch_infos.get("id") or inc_batch_infos.get("batch_id")) if isinstance(inc_batch_infos, dict) else (inc_batch_infos if isinstance(inc_batch_infos, str) else None)
        inc_batch_name = inc_batch_infos.get("name") if isinstance(inc_batch_infos, dict) else (inc_batch_infos if isinstance(inc_batch_infos, str) else None)

        def _find_b(b_list, target_id, target_name):
            for b in (b_list or []):
                if not isinstance(b, dict): continue
                c_id, c_name = b.get("id"), b.get("name")
                if (target_id and (c_id == target_id or c_name == target_id)) or (target_name and (c_name == target_name or c_id == target_name)):
                    return b
            return {}

        db_serialno_infos = []
        if has_variant and inc_variant_id:
            variant_data = prod_db.get("variants", {}).get(inc_variant_id) or {}
            if has_batch and (inc_batch_id or inc_batch_name):
                batches_list = variant_data.get("batch_infos") or []
                batch_data = _find_b(batches_list, inc_batch_id, inc_batch_name)
                db_serialno_infos = batch_data.get("serialno_infos") or []
                if not db_serialno_infos:
                    db_serialno_infos = variant_data.get("serialno_infos") or []
                    if not db_serialno_infos:
                        for b in batches_list:
                            if isinstance(b, dict) and b.get("serialno_infos"):
                                db_serialno_infos.extend(b["serialno_infos"])
            else:
                db_serialno_infos = variant_data.get("serialno_infos") or []
                if not db_serialno_infos and has_batch:
                    for b in (variant_data.get("batch_infos") or []):
                        if isinstance(b, dict) and b.get("serialno_infos"):
                            db_serialno_infos.extend(b["serialno_infos"])

        elif has_batch and (inc_batch_id or inc_batch_name):
            batches_list = prod_db.get("batch_infos") or []
            batch_data = _find_b(batches_list, inc_batch_id, inc_batch_name)
            db_serialno_infos = batch_data.get("serialno_infos") or []
            if not db_serialno_infos:
                db_serialno_infos = prod_db.get("serialno_infos") or []
                if not db_serialno_infos:
                    for b in batches_list:
                        if isinstance(b, dict) and b.get("serialno_infos"):
                            db_serialno_infos.extend(b["serialno_infos"])
        else:
            db_serialno_infos = prod_db.get("serialno_infos") or []

        if not db_serialno_infos:
            if prod_db.get("serialno_infos"):
                db_serialno_infos.extend(prod_db["serialno_infos"])
            for v in (prod_db.get("variants") or {}).values():
                if isinstance(v, dict):
                    if v.get("serialno_infos"):
                        db_serialno_infos.extend(v["serialno_infos"])
                    for b in (v.get("batch_infos") or []):
                        if isinstance(b, dict) and b.get("serialno_infos"):
                            db_serialno_infos.extend(b["serialno_infos"])
            for b in (prod_db.get("batch_infos") or []):
                if isinstance(b, dict) and b.get("serialno_infos"):
                    db_serialno_infos.extend(b["serialno_infos"])

        db_sn_id_by_name = {}
        db_sn_ids_set = set()
        for sn in db_serialno_infos:
            if isinstance(sn, dict):
                s_id = sn.get("id") or sn.get("serialno_id")
                s_name = sn.get("name") or sn.get("serialno_name") or sn.get("serial_no")
                if s_id:
                    db_sn_ids_set.add(str(s_id))
                if s_name and s_id:
                    db_sn_id_by_name[str(s_name).strip()] = str(s_id)

        serialno_todelete = []
        for serialno in inc_serialnos:
            sn_id = None
            sn_name = None
            if isinstance(serialno, dict):
                sn_id = serialno.get("id") or serialno.get("serialno_id") or serialno.get("serial_no_id")
                sn_name = serialno.get("name") or serialno.get("serialno_name") or serialno.get("serial_no_name") or serialno.get("serial_no") or serialno.get("serialno")
            elif isinstance(serialno, str):
                if serialno in db_sn_ids_set:
                    sn_id = serialno
                else:
                    sn_name = serialno

            target_id = None
            if sn_id and str(sn_id) in db_sn_ids_set:
                target_id = str(sn_id)
            elif sn_name and str(sn_name).strip() in db_sn_id_by_name:
                target_id = db_sn_id_by_name[str(sn_name).strip()]
            elif sn_id and str(sn_id).strip() in db_sn_id_by_name:
                target_id = db_sn_id_by_name[str(sn_id).strip()]

            if target_id:
                serialno_todelete.append(target_id)
        return serialno_todelete

    # Case 1: Base product
    res1 = resolve_serials(prod_db_base, None, {}, [{"id": "s1_uuid", "name": "SN-BASE-001"}])
    ic("Case 1 (Base):", res1)
    assert res1 == ["s1_uuid"]

    # Case 2: Variant product
    res2 = resolve_serials(prod_db_variant, "v1", {}, [{"id": "s2_uuid", "name": "SN-VAR-001"}])
    ic("Case 2 (Variant):", res2)
    assert res2 == ["s2_uuid"]

    # Case 3: Batch product
    res3 = resolve_serials(prod_db_batch, None, {"id": "b1", "name": "BATCH-100"}, [{"id": "s3_uuid", "name": "SN-BATCH-001"}])
    ic("Case 3 (Batch):", res3)
    assert res3 == ["s3_uuid"]

    # Case 4: Variant + Batch product
    res4 = resolve_serials(prod_db_var_batch, "v2", {"id": "b2", "name": "BATCH-200"}, [{"id": "s4_uuid", "name": "SN-VARBATCH-001"}])
    ic("Case 4 (Var+Batch):", res4)
    assert res4 == ["s4_uuid"]

    print("ALL 4 PRODUCT DECREMENT SERIAL RESOLUTION CASES PASSED CLEANLY!")

if __name__ == "__main__":
    test_decrement_serial_resolution()
