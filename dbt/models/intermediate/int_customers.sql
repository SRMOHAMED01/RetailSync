-- Intermediate: cleaned customer entity.
-- Adds a derived full_name and normalizes email casing.
select
    customer_id,
    concat(first_name, ' ', last_name) as full_name,
    first_name,
    last_name,
    lower(email) as email,
    city,
    created_at,
    updated_at
from {{ ref('stg_customers') }}