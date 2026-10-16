{{ config(materialized='table') }}

-- Dimension: product (current state from Silver).
select
    product_id,
    name,
    category,
    price,
    stock,
    updated_at
from {{ source('retailsync_silver', 'products') }}