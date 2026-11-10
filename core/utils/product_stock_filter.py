from typing import Any, Tuple

def is_truthy(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes")
    return bool(val)


def compute_product_stock_details(doc: dict) -> Tuple[float, float, bool, bool, bool, bool]:
    """
    Returns (total_stock, total_rop, found_stock, has_out_of_stock, has_low_stock, has_in_stock)
    Evaluates individual variants and batches for out of stock, low stock, and in stock flags.
    """
    have_tracking = doc.get("have_tracking", True)
    if have_tracking is False:
        return 0.0, 0.0, False, False, False, True

    type_infos = doc.get("type_infos") or {}
    has_variant = type_infos.get("has_variant", False)
    has_batch = type_infos.get("has_batch", False)

    total_stock = 0.0
    total_rop = 0.0
    found_stock = False

    has_out_of_stock = False
    has_low_stock = False
    has_in_stock = False

    parent_rop = None
    if doc.get("reorder_point_infos"):
        parent_rop = doc.get("reorder_point_infos", {}).get("reorder_point")
    if parent_rop is None and doc.get("reorder_point") is not None:
        parent_rop = doc.get("reorder_point")
    parent_rop_val = float(parent_rop) if parent_rop is not None and str(parent_rop).strip() != "" else 10.0

    variants = doc.get("variants")
    variant_list = []
    if isinstance(variants, dict) and variants:
        variant_list = list(variants.values())
    elif isinstance(variants, list) and variants:
        variant_list = variants

    if variant_list:
        for v_data in variant_list:
            if not isinstance(v_data, dict):
                continue
            v_batches = v_data.get("batch_infos") if (has_batch and isinstance(v_data.get("batch_infos"), list)) else []
            v_stk_val = 0.0
            v_rop_val = None

            if v_batches:
                v_batch_total = 0.0
                for b in v_batches:
                    if isinstance(b, dict):
                        stk = b.get("stock_infos", {})
                        rop = b.get("reorder_point_infos", {})
                        b_val = 0.0
                        if stk:
                            raw_stk = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                            if raw_stk is not None:
                                b_val = float(raw_stk)
                                found_stock = True
                        elif b.get("stocks") is not None:
                            b_val = float(b.get("stocks") or 0.0)
                            found_stock = True
                        elif b.get("quantity") is not None:
                            b_val = float(b.get("quantity") or 0.0)
                            found_stock = True

                        if rop and rop.get("reorder_point") is not None:
                            b_rop = float(rop.get("reorder_point"))
                        elif b.get("reorder_point") is not None:
                            b_rop = float(b.get("reorder_point"))
                        else:
                            b_rop = parent_rop_val

                        v_batch_total += b_val
                        if b_val <= 0:
                            has_out_of_stock = True
                        elif b_val <= b_rop:
                            has_low_stock = True
                        else:
                            has_in_stock = True

                v_stk_val = v_batch_total
            else:
                stk = v_data.get("stock_infos", {})
                rop = v_data.get("reorder_point_infos", {})
                if stk:
                    raw_stk = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                    if raw_stk is not None:
                        v_stk_val = float(raw_stk)
                        found_stock = True
                elif v_data.get("stocks") is not None:
                    v_stk_val = float(v_data.get("stocks") or 0.0)
                    found_stock = True
                elif v_data.get("stock") is not None:
                    v_stk_val = float(v_data.get("stock") or 0.0)
                    found_stock = True

                if rop and rop.get("reorder_point") is not None:
                    v_rop_val = float(rop.get("reorder_point"))
                elif v_data.get("reorder_point") is not None:
                    v_rop_val = float(v_data.get("reorder_point"))
                else:
                    v_rop_val = parent_rop_val

                if v_stk_val <= 0:
                    has_out_of_stock = True
                elif v_stk_val <= v_rop_val:
                    has_low_stock = True
                else:
                    has_in_stock = True

            total_stock += v_stk_val
            if v_rop_val is not None:
                total_rop += v_rop_val

    elif has_batch and isinstance(doc.get("batch_infos"), list) and doc.get("batch_infos"):
        for b in doc.get("batch_infos", []):
            if isinstance(b, dict):
                stk = b.get("stock_infos", {})
                rop = b.get("reorder_point_infos", {})
                b_val = 0.0
                if stk:
                    raw_val = stk.get("available_stocks") if stk.get("available_stocks") is not None else (stk.get("physical_stocks") if stk.get("physical_stocks") is not None else stk.get("stocks"))
                    if raw_val is not None:
                        b_val = float(raw_val)
                        found_stock = True
                elif b.get("stocks") is not None:
                    b_val = float(b.get("stocks") or 0.0)
                    found_stock = True
                elif b.get("quantity") is not None:
                    b_val = float(b.get("quantity") or 0.0)
                    found_stock = True

                if rop and rop.get("reorder_point") is not None:
                    b_rop = float(rop.get("reorder_point"))
                elif b.get("reorder_point") is not None:
                    b_rop = float(b.get("reorder_point"))
                else:
                    b_rop = parent_rop_val

                total_stock += b_val
                total_rop += b_rop

                if b_val <= 0:
                    has_out_of_stock = True
                elif b_val <= b_rop:
                    has_low_stock = True
                else:
                    has_in_stock = True

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

        if rop and rop.get("reorder_point") is not None:
            total_rop = float(rop.get("reorder_point"))
        elif doc.get("reorder_point") is not None:
            total_rop = float(doc.get("reorder_point") or 0.0)
        else:
            total_rop = parent_rop_val

        if total_stock <= 0:
            has_out_of_stock = True
        elif total_stock <= total_rop:
            has_low_stock = True
        else:
            has_in_stock = True

    return total_stock, total_rop, found_stock, has_out_of_stock, has_low_stock, has_in_stock


def compute_product_stock_and_rop(doc: dict) -> Tuple[float, float, bool]:
    """
    Returns (total_available_stock, total_reorder_point, has_stock_info)
    Computes total available stocks and total reorder points across variants/batches.
    """
    total_stock, total_rop, found_stock, _, _, _ = compute_product_stock_details(doc)
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
        _, _, _, has_out_of_stock, has_low_stock, has_in_stock = compute_product_stock_details(d)
        # Priority-based overall status:
        # 1 - Out of stock (if any variant/batch is out of stock)
        # 2 - Low stock (if any variant/batch is low stock, and none is out of stock)
        # 3 - In stock
        if has_out_of_stock:
            is_out_of_stock = True
            is_low_stock = False
            is_in_stock = False
        elif has_low_stock:
            is_out_of_stock = False
            is_low_stock = True
            is_in_stock = False
        else:
            is_out_of_stock = False
            is_low_stock = False
            is_in_stock = True
    else:
        is_out_of_stock = False
        is_low_stock = False
        is_in_stock = True
        has_out_of_stock = False
        has_low_stock = False
        has_in_stock = True

    if exclude_stocks and not (has_out_of_stock or has_low_stock):
        return False
    if exclude_outofstock and is_out_of_stock and not has_low_stock:
        return False
    if exclude_low_stocks and is_low_stock and not has_out_of_stock:
        return False

    if stock_status:
        status_val = str(stock_status).lower().strip()
        if status_val in ["no", "no_stock", "out_of_stock", "outofstock"]:
            if not (is_out_of_stock or has_out_of_stock):
                return False
        elif status_val in ["low", "low_stock", "lowstock", "low_stocks"]:
            if not (is_low_stock or has_low_stock):
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

