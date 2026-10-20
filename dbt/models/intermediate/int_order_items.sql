-- Intermediate: cleaned order_item entity.
-- Adds derived line_total for the fact layer.
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as line_total
from {{ ref('stg_order_items') }}