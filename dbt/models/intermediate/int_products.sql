-- Intermediate: cleaned product entity.
-- Adds a derived category_upper for case-insensitive grouping and keeps the
-- raw price for downstream money math.
select
    product_id,
    name,
    category,
    upper(category) as category_upper,
    price,
    stock,
    updated_at
from {{ ref('stg_products') }}