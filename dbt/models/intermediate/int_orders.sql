-- Intermediate: cleaned order entity.
-- Adds a derived order_year/month for time slicing and flags the status.
select
    order_id,
    customer_id,
    status,
    total_amount,
    cast(total_amount * 100 as long) as total_amount_cents,
    created_at,
    updated_at,
    date(created_at) as order_date,
    year(created_at) as order_year,
    month(created_at) as order_month
from {{ ref('stg_orders') }}