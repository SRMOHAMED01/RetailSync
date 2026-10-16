-- Staging: order view over the Silver current state.
-- Mirrors the raw Silver columns so downstream marts can `ref('stg_orders')`.
select
    order_id,
    customer_id,
    status,
    total_amount,
    ingested_at
from {{ source('retailsync_silver', 'orders') }}