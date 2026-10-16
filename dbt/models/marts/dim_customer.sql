{{ config(materialized='table') }}

-- Dimension: customer (SCD Type 1 current state from Silver).
select
    customer_id,
    first_name,
    last_name,
    email,
    city,
    created_at,
    updated_at
from {{ source('retailsync_silver', 'customers') }}