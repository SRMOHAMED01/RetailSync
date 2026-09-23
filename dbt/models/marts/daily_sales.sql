select cast(ingested_at as date) as sales_date,
       count(distinct order_id) as orders_count,
       sum(total_amount) as revenue,
       avg(total_amount) as avg_order_value
from {{ ref('stg_orders') }}
where status in ('PAID','SHIPPED','DELIVERED')
group by 1
