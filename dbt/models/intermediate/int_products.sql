-- Intermediate: cleaned product entity.
-- Adds a derived unit_price_cents column for consistent money math.
select
    product_id,
    name,
    category,
    price,
    cast(price * 100 as long) as price_cents,
    stock,
    updated_at
from {{ ref('stg_products') }}