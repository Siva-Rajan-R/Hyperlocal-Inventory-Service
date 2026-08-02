import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from icecream import ic

def test_direct_id_resolution():
    inc_serialnos = [
        {"id": "36c55ad1-f7d3-572e-9322-a1e1a3ed4c69"}
    ]

    serialno_todelete = []
    for serialno in inc_serialnos:
        sn_id = None
        sn_name = None
        if isinstance(serialno, dict):
            sn_id = serialno.get("id") or serialno.get("serialno_id") or serialno.get("serial_no_id")
            sn_name = serialno.get("name") or serialno.get("serialno_name") or serialno.get("serial_no_name") or serialno.get("serial_no") or serialno.get("serialno")
        elif isinstance(serialno, str):
            if len(serialno) > 20 or "-" in serialno:
                sn_id = serialno
            else:
                sn_name = serialno

        target_id = None
        if sn_id:
            target_id = str(sn_id)
        elif sn_name and str(sn_name).strip() in db_sn_id_by_name:
            target_id = db_sn_id_by_name[str(sn_name).strip()]

        if target_id:
            serialno_todelete.append(target_id)

    ic("Direct ID to delete =>", serialno_todelete)
    assert serialno_todelete == ["36c55ad1-f7d3-572e-9322-a1e1a3ed4c69"]
    print("DIRECT ID RESOLUTION TEST PASSED CLEANLY!")

if __name__ == "__main__":
    test_direct_id_resolution()
