from typing import List, Dict, Optional
from fastapi import HTTPException
from icecream import ic
from hyperlocal_platform.core.models.req_res_models import ErrorResponseTypDict

def validate_and_filter_custom_fields(
    payload_custom_fields: Dict, 
    defined_custom_fields: List[Dict],
    category_id: Optional[str] = None
) -> Dict:
    """
    Validates the provided custom fields against the defined custom fields.
    - If no defined custom fields, ignore the payload and return {}.
    - If a field is scoped to a specific category_id, it is only applicable to products in that category.
    - Checks for missing required fields (only for fields applicable to the product's category) and raises HTTPException.
    - Filters out any fields in payload that are not defined or belong strictly to other categories.
    """
    if not defined_custom_fields:
        return {}

    payload_custom_fields = payload_custom_fields or {}

    # Separate applicable fields (global or matching this product's category)
    applicable_fields_by_name = {}
    applicable_fields_by_id = {}
    applicable_field_list = []

    for field in defined_custom_fields:
        field_cat = field.get("category_id")
        # Global field if category_id is missing, None, or empty string / "all"
        is_global = not field_cat or str(field_cat).strip() == "" or str(field_cat).strip().lower() == "all"
        is_category_match = bool(category_id and str(field_cat).strip() == str(category_id).strip())

        if is_global or is_category_match:
            applicable_fields_by_name[field['field_name']] = field
            applicable_fields_by_id[field['id']] = field
            applicable_field_list.append(field)

    ic(defined_custom_fields, category_id, applicable_field_list, payload_custom_fields)

    # Check for required fields among applicable fields only
    for field_def in applicable_field_list:
        field_name = field_def.get('field_name')
        field_id = field_def.get('id')
        field_type = str(field_def.get('type') or field_def.get('field_type') or '').strip().lower()

        val = None
        if field_name in payload_custom_fields:
            val = payload_custom_fields.get(field_name)
        elif field_id in payload_custom_fields:
            val = payload_custom_fields.get(field_id)

        # For boolean fields, unchecked is represented as False. Default to False if omitted or empty.
        if field_type == 'boolean':
            if val is None or str(val).strip() == "":
                payload_custom_fields[field_name] = False
            continue

        if field_def.get('required'):
            if val is None or str(val).strip() == "":
                label = field_def.get('label_name') or field_name
                raise HTTPException(
                    status_code=400,
                    detail=ErrorResponseTypDict(
                        status_code=400,
                        msg="Missing Required Custom Field",
                        description=f"The custom field '{label}' is required for this product category.",
                        success=False
                    )
                )

    # Filter out unknown or non-applicable fields
    valid_custom_fields = {}
    for key, value in payload_custom_fields.items():
        if key in applicable_fields_by_name:
            field_id = applicable_fields_by_name[key]['id']
            valid_custom_fields[field_id] = value
        elif key in applicable_fields_by_id:
            valid_custom_fields[key] = value

    ic(valid_custom_fields)
    return valid_custom_fields
