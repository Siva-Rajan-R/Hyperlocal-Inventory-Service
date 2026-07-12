import re
from typing import Optional
from sqlalchemy import select
from infras.primary_db.models.product_model import Products, ProductVariants
from sqlalchemy.ext.asyncio import AsyncSession
from integrations.utility_service import get_shop_category

async def generate_product_sku(
    session: AsyncSession,
    shop_id: str,
    category_id: str,
    product_name: str,
    variant_name: Optional[str] = None
) -> str:
    # 1. catCode = first 3 letters of category, uppercased, A–Z only
    cat_name = "GEN"
    if category_id:
        try:
            cat_data = await get_shop_category(shop_id=shop_id, category_id=category_id)
            if cat_data and isinstance(cat_data, dict):
                cat_name = cat_data.get("name", "GEN")
        except Exception:
            pass
            
    cat_clean = re.sub(r'[^A-Z]', '', cat_name.upper())
    cat_code = cat_clean[:3] if len(cat_clean) >= 3 else (cat_clean + "XXX")[:3]

    # 2. nameCode = first 3–4 letters of the main product word, uppercased, A–Z only
    name_words = [w for w in product_name.split() if w]
    main_word = name_words[0] if name_words else "PROD"
    name_clean = re.sub(r'[^A-Z]', '', main_word.upper())
    name_code = name_clean[:4] if len(name_clean) >= 3 else (name_clean + "XXX")[:3]

    # 3. varCode = variant attribute → uppercase, remove spaces/symbols
    var_code = None
    if variant_name:
        var_clean = re.sub(r'[^A-Z0-9]', '', variant_name.upper())
        if var_clean:
            var_code = var_clean

    # 4. base = catCode + "-" + nameCode + (varCode ? "-" + varCode : "")
    base = f"{cat_code}-{name_code}"
    if var_code:
        base = f"{base}-{var_code}"

    # 5. tail = next sequential number for that base (0001, 0002…)
    # 6. SKU = base + "-" + tail
    # 7. CHECK uniqueness loop
    counter = 1
    while True:
        tail = f"{counter:04d}"
        candidate_sku = f"{base}-{tail}"
        
        # Check if candidate_sku already exists in Products table
        prod_stmt = select(Products.id).where(Products.shop_id == shop_id, Products.sku == candidate_sku)
        prod_exists = (await session.execute(prod_stmt)).scalars().first()
        
        # Check if candidate_sku already exists in ProductVariants table
        var_stmt = select(ProductVariants.id).where(ProductVariants.shop_id == shop_id, ProductVariants.sku == candidate_sku)
        var_exists = (await session.execute(var_stmt)).scalars().first()
        
        if not prod_exists and not var_exists:
            return candidate_sku
        counter += 1

async def validate_sku_uniqueness(
    session: AsyncSession,
    shop_id: str,
    sku: str,
    exclude_product_id: Optional[str] = None,
    exclude_variant_id: Optional[str] = None
) -> bool:
    # Check Products table
    prod_query = select(Products.id).where(Products.shop_id == shop_id, Products.sku == sku)
    if exclude_product_id:
        prod_query = prod_query.where(Products.id != exclude_product_id)
    prod_exists = (await session.execute(prod_query)).scalars().first()
    if prod_exists:
        return False

    # Check ProductVariants table
    var_query = select(ProductVariants.id).where(ProductVariants.shop_id == shop_id, ProductVariants.sku == sku)
    if exclude_variant_id:
        var_query = var_query.where(ProductVariants.id != exclude_variant_id)
    var_exists = (await session.execute(var_query)).scalars().first()
    if var_exists:
        return False

    return True

async def generate_product_barcode(session: AsyncSession, shop_id: str) -> str:
    counter = 1
    while True:
        candidate_barcode = f"BAR{counter:06d}"
        
        # Check Products table
        prod_stmt = select(Products.id).where(Products.shop_id == shop_id, Products.barcode == candidate_barcode)
        prod_exists = (await session.execute(prod_stmt)).scalars().first()
        
        # Check ProductVariants table
        var_stmt = select(ProductVariants.id).where(ProductVariants.shop_id == shop_id, ProductVariants.barcode == candidate_barcode)
        var_exists = (await session.execute(var_stmt)).scalars().first()
        
        if not prod_exists and not var_exists:
            return candidate_barcode
        counter += 1

async def validate_barcode_uniqueness(
    session: AsyncSession,
    shop_id: str,
    barcode: str,
    exclude_product_id: Optional[str] = None,
    exclude_variant_id: Optional[str] = None
) -> bool:
    if not barcode:
        return True
        
    # Check Products table
    prod_query = select(Products.id).where(Products.shop_id == shop_id, Products.barcode == barcode)
    if exclude_product_id:
        prod_query = prod_query.where(Products.id != exclude_product_id)
    prod_exists = (await session.execute(prod_query)).scalars().first()
    if prod_exists:
        return False

    # Check ProductVariants table
    var_query = select(ProductVariants.id).where(ProductVariants.shop_id == shop_id, ProductVariants.barcode == barcode)
    if exclude_variant_id:
        var_query = var_query.where(ProductVariants.id != exclude_variant_id)
    var_exists = (await session.execute(var_query)).scalars().first()
    if var_exists:
        return False

    return True
