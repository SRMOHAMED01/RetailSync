{{ config(materialized='table') }}

-- Fact: sales grain = one row per order_item.
-- Joins the intermediate order_items to dimensions (product, customer, date).
select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    dd.date_id,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    o.status as order_status,
    o.order_date as sales_date
from {{ ref('int_order_items') }} oi
join {{ ref('int_orders') }} o
    on o.order_id = oi.order_id
join {{ ref('dim_customer') }} c
    on c.customer_id = o.customer_id
join {{ ref('dim_product') }} p
    on p.product_id = oi.product_id
join {{ ref('dim_date') }} dd
    on dd.date_id = o.order_date