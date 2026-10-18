-- Staging: order_item view over the Silver current state.
-- Mirrors the raw Silver columns so downstream marts can `ref('stg_order_items')`.
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price
from {{ source('retailsync_silver', 'order_items') }}