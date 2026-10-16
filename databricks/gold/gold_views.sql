-- Databricks SQL / dbt Gold — analytics views for Power BI.
-- These are the marts Power BI should consume (see powerbi/README.md).
-- They are thin wrappers over the dbt Gold models so Power BI can query them
-- directly from the Databricks SQL Warehouse without needing dbt access.

-- 1) Executive Overview: revenue, orders, average basket, trend
CREATE OR REPLACE VIEW retailsync.gold.daily_sales_bi AS
select
    cast(ingested_at as date) as sales_date,
    count(distinct order_id) as orders_count,
    sum(total_amount) as revenue,
    avg(total_amount) as avg_order_value
from retailsync.silver.orders
where status in ('PAID', 'SHIPPED', 'DELIVERED')
group by 1;

-- 2) Product Analytics: revenue by product
CREATE OR REPLACE VIEW retailsync.gold.product_sales_bi AS
select
    p.product_id,
    p.name as product_name,
    p.category,
    sum(oi.quantity) as units_sold,
    sum(oi.quantity * oi.unit_price) as revenue
from retailsync.silver.order_items oi
join retailsync.silver.products p on p.product_id = oi.product_id
group by 1, 2, 3;

-- 3) Customer 360: per-customer spend + order count
CREATE OR REPLACE VIEW retailsync.gold.customer_360_bi AS
select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    count(o.order_id) as orders_count,
    coalesce(sum(o.total_amount), 0) as lifetime_spend,
    coalesce(avg(o.total_amount), 0) as avg_basket
from retailsync.silver.customers c
left join retailsync.silver.orders o on o.customer_id = c.customer_id
group by 1, 2, 3, 4;