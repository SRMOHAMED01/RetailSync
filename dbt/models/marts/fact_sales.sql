{{ config(materialized='table') }}

-- Fact: sales grain = one row per order_item.
-- Joins the Silver order_items to dimensions (product, order, date).
select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) as line_total,
    o.status as order_status,
    o.created_at as order_created_at,
    date(o.created_at) as sales_date
from {{ source('retailsync_silver', 'order_items') }} oi
join {{ source('retailsync_silver', 'orders') }} o
    on o.order_id = oi.order_id