-- Intermediate: cleaned payment entity.
-- Adds derived amount_cents and payment month for reconciliation.
select
    payment_id,
    order_id,
    amount,
    cast(amount * 100 as long) as amount_cents,
    payment_method,
    status,
    created_at,
    date(created_at) as payment_date,
    year(created_at) as payment_year,
    month(created_at) as payment_month
from {{ ref('stg_payments') }}