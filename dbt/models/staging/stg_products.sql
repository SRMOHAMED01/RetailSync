-- Staging: product view over the Silver current state.
-- Mirrors the raw Silver columns so downstream marts can `ref('stg_products')`.
select
    product_id,
    name,
    category,
    price,
    stock,
    updated_at
from {{ source('retailsync_silver', 'products') }}