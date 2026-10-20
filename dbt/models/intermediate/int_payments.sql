-- Intermediate: cleaned payment entity.
-- Adds derived payment month for reconciliation.
select
    payment_id,
    order_id,
    amount,
    payment_method,
    status,
    created_at,
    date(created_at) as payment_date,
    year(created_at) as payment_year,
    month(created_at) as payment_month
from {{ ref('stg_payments') }}