-- Staging: payment view over the Silver current state.
-- Mirrors the raw Silver columns so downstream marts can `ref('stg_payments')`.
select
    payment_id,
    order_id,
    amount,
    payment_method,
    status,
    created_at
from {{ source('retailsync_silver', 'payments') }}