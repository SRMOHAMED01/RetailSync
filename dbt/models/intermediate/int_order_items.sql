-- Intermediate: cleaned order_item entity.
-- Adds derived line_total and joins product_id for convenience.
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    cast(unit_price * 100 as long) as unit_price_cents,
    quantity * unit_price as line_total,
    cast(quantity * unit_price * 100 as long) as line_total_cents
from {{ ref('stg_order_items') }}