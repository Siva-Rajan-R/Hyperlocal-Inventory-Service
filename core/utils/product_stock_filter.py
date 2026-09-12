from typing import Any, Tuple

def is_truthy(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes")
    return bool(val)


def compute_product_stock_and_rop(doc: dict) -> Tuple[float, float, bool]:
    """
    Returns (total_available_stock, total_reorder_point, has_stock_info)
    Computes total available stocks and total reorder points across variants/batches.
    """
    have_tracking = doc.get("have_tracking", True)
    if have_tracking is False:
        return 0.0, 0.0, False

    type_infos = doc.get("type_infos") or {}
    has_variant = type_infos.get("has_variant", False)
    has_batch = type_infos.get("has_batch", False)

    total_stock = 0.0
    total_rop = 0.0
    found_stock = False

    variants = doc.get("variants")
    if isinstance(variants, dict) and variants:
        for v_id, v_data in variants.items():
            if not isinstance(v_data, dict):
                continue
            if has_batch and v_data.get("batch_infos"):
                for b in v_data.get("batch_infos", []):
                    if isinstance(b, dict):
                        stk = b.get("stock_infos", {})
                        rop = b.get("reorder_point_infos", {})
                        if stk:
                            val = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                            if val is not None:
                                total_stock += float(val)
                                found_stock = True
                        if rop:
                            rop_val = rop.get("reorder_point")
                            if rop_val is not None:
                                total_rop += float(rop_val)
            else:
                stk = v_data.get("stock_infos", {})
                rop = v_data.get("reorder_point_infos", {})
                if stk:
                    val = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                    if val is not None:
                        total_stock += float(val)
                        found_stock = True
                if rop:
                    rop_val = rop.get("reorder_point")
                    if rop_val is not None:
                        total_rop += float(rop_val)
    elif isinstance(variants, list) and variants:
        for v_data in variants:
            if not isinstance(v_data, dict):
                continue
            if has_batch and v_data.get("batch_infos"):
                for b in v_data.get("batch_infos", []):
                    if isinstance(b, dict):
                        stk = b.get("stock_infos", {})
                        rop = b.get("reorder_point_infos", {})
                        if stk:
                            val = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                            if val is not None:
                                total_stock += float(val)
                                found_stock = True
                        if rop:
                            rop_val = rop.get("reorder_point")
                            if rop_val is not None:
                                total_rop += float(rop_val)
            else:
                stk = v_data.get("stock_infos", {})
                rop = v_data.get("reorder_point_infos", {})
                if stk:
                    val = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                    if val is not None:
                        total_stock += float(val)
                        found_stock = True
                if rop:
                    rop_val = rop.get("reorder_point")
                    if rop_val is not None:
                        total_rop += float(rop_val)
    elif has_batch and doc.get("batch_infos"):
        for b in doc.get("batch_infos", []):
            if isinstance(b, dict):
                stk = b.get("stock_infos", {})
                rop = b.get("reorder_point_infos", {})
                if stk:
                    val = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                    if val is not None:
                        total_stock += float(val)
                        found_stock = True
                if rop:
                    rop_val = rop.get("reorder_point")
                    if rop_val is not None:
                        total_rop += float(rop_val)
    else:
        stk = doc.get("stock_infos", {})
        rop = doc.get("reorder_point_infos", {})
        if stk:
            val = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
            if val is not None:
                total_stock += float(val)
                found_stock = True
        elif doc.get("stocks") is not None:
            total_stock += float(doc.get("stocks") or 0.0)
            found_stock = True
        elif doc.get("stock") is not None:
            total_stock += float(doc.get("stock") or 0.0)
            found_stock = True

        if rop:
            rop_val = rop.get("reorder_point")
            if rop_val is not None:
                total_rop += float(rop_val)
        elif doc.get("reorder_point") is not None:
            total_rop += float(doc.get("reorder_point") or 0.0)

    return total_stock, total_rop, found_stock


def filter_product_item(d: dict, data: Any) -> bool:
    """
    Evaluates whether a product dictionary satisfies all exclusion and inclusion filters in request schema `data`.
    """
    is_active = d.get("is_active", True)
    if is_truthy(getattr(data, 'exclude_inactive', None)) and not is_active:
        return False
    if is_truthy(getattr(data, 'exclude_active', None)) and is_active:
        return False

    have_tracking = d.get("have_tracking", True)
    if is_truthy(getattr(data, 'exclude_tracking', None)) and (have_tracking is not False):
        return False
    if is_truthy(getattr(data, 'exclude_non_tracking', None)) and (have_tracking is False):
        return False

    if getattr(data, 'active', None) is not None and is_active != is_truthy(data.active):
        return False
    if getattr(data, 'have_tracking', None) is not None and have_tracking != is_truthy(data.have_tracking):
        return False

    # Stock exclusions and filtering
    exclude_stocks_val = (
        getattr(data, 'exclude_stocks', None) or
        getattr(data, 'exclude_in_stock', None) or
        getattr(data, 'exclude_stock', None)
    )
    exclude_stocks = is_truthy(exclude_stocks_val)

    exclude_outofstock_val = (
        getattr(data, 'exclude_outofstock', None) or
        getattr(data, 'exclude_out_of_stock', None) or
        getattr(data, 'exclude_outofstov', None) or
        getattr(data, 'exclude_no_stock', None)
    )
    exclude_outofstock = is_truthy(exclude_outofstock_val)

    exclude_low_stocks_val = (
        getattr(data, 'exclude_low_stocks', None) or
        getattr(data, 'exclude_low_stock', None) or
        getattr(data, 'exclude_lowstocks', None) or
        getattr(data, 'exclude_lowstock', None)
    )
    exclude_low_stocks = is_truthy(exclude_low_stocks_val)

    stock_status = getattr(data, 'stock_status', None)

    if have_tracking is not False:
        total_stock, total_rop, _ = compute_product_stock_and_rop(d)
        is_out_of_stock = (total_stock <= 0)
        is_low_stock = (total_stock > 0 and total_stock <= total_rop)
        is_in_stock = (total_stock > total_rop and total_stock > 0)

        if exclude_stocks and is_in_stock:
            return False
        if exclude_outofstock and is_out_of_stock:
            return False
        if exclude_low_stocks and is_low_stock:
            return False

        if stock_status:
            status_val = str(stock_status).lower().strip()
            if status_val in ["no", "no_stock", "out_of_stock", "outofstock"]:
                if not is_out_of_stock:
                    return False
            elif status_val in ["low", "low_stock", "lowstock", "low_stocks"]:
                if not is_low_stock:
                    return False
            elif status_val in ["in_stock", "instock", "stock", "stocks"]:
                if not is_in_stock:
                    return False

    return True


def has_stock_filter(data: Any) -> bool:
    for attr in (
        'exclude_stocks', 'exclude_in_stock', 'exclude_stock',
        'exclude_outofstock', 'exclude_out_of_stock', 'exclude_outofstov', 'exclude_no_stock',
        'exclude_low_stocks', 'exclude_low_stock', 'exclude_lowstocks', 'exclude_lowstock',
        'stock_status'
    ):
        val = getattr(data, attr, None)
        if val is not None and is_truthy(val):
            return True
    return bool(getattr(data, 'stock_status', None))

