{{ config(materialized='table') }}

-- Fact: orders grain = one row per order, enriched with customer + date.
select
    o.order_id,
    o.customer_id,
    o.status,
    o.total_amount,
    o.created_at,
    o.updated_at,
    date(o.created_at) as sales_date,
    c.city as customer_city
from {{ source('retailsync_silver', 'orders') }} o
join {{ source('retailsync_silver', 'customers') }} c
    on c.customer_id = o.customer_id