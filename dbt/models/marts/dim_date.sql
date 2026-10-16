{{ config(materialized='table') }}

-- Dimension: date spine derived from order activity (Silver).
with orders as (
    select ingested_at from {{ source('retailsync_silver', 'orders') }}
),

bounds as (
    select
        min(date(ingested_at)) as min_date,
        max(date(ingested_at)) as max_date
    from orders
),

spine as (
    select explode(
        sequence(
            (select min_date from bounds),
            (select max_date from bounds),
            interval 1 day
        )
    ) as date_day
)

select
    date_day as date_id,
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    day(date_day) as day,
    date_format(date_day, 'EEEE') as day_name,
    date_format(date_day, 'MMM') as month_name,
    (dayofweek(date_day) in (1, 7)) as is_weekend
from spine