-- Staging: customer view over the Silver current state.
-- Mirrors the raw Silver columns so downstream marts can `ref('stg_customers')`.
select
    customer_id,
    first_name,
    last_name,
    email,
    city,
    created_at,
    updated_at
from {{ source('retailsync_silver', 'customers') }}