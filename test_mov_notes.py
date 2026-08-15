data = [
    {
        "product_id": "test_product",
        "shop_id": "test_shop",
        "ui_id": "RET-1000",
        "purchase_id": "PUR-999",
        "type": "DECREMENT",
        "entity_name": "OFFLINE_PURCHASE_RETURN",
        "stocks": 5
    }
]

entity_id_val = None
update_type_val = None
entity_name = data[0].get('entity_name')

if data:
    for d in data:
        if isinstance(d, dict):
            if not entity_id_val:
                entity_id_val = d.get('purchase_ui_id') or d.get('order_ui_id') or d.get('ui_id') or d.get('entity_id') or d.get('invoice_no') or d.get('sale_ui_id') or d.get('return_ui_id') or d.get('sale_return_ui_id') or d.get('offline_sale_ui_id') or d.get('order_id') or d.get('sale_id') or d.get('return_id')
            if not update_type_val:
                update_type_val = d.get('type')
            if entity_id_val and update_type_val:
                break

desc_entity = entity_name.replace("_", " ").lower() if entity_name else "adjustment"

if update_type_val == "INCREMENT":
    action_text = "Stock increase"
elif update_type_val == "DECREMENT":
    action_text = "Stock decrease"
else:
    action_text = "Stock adjusted"

if entity_id_val:
    desc_str = f"{action_text} via {desc_entity} and its id {entity_id_val}"
else:
    desc_str = f"{action_text} via {desc_entity}"

print(f"Resulting desc_str: {desc_str}")
