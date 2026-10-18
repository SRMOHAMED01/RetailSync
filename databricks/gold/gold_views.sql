-- Databricks SQL / dbt Gold — analytics views for Power BI.
-- These are thin wrappers over the dbt Gold models so Power BI can query them
-- directly from the Databricks SQL Warehouse.
--
-- The underlying dbt marts live in the `retailsync` schema:
--   retailsync.fact_sales     (one row per order_item)
--   retailsync.fact_orders    (one row per order)
--   retailsync.dim_customer
--   retailsync.dim_product
--   retailsync.dim_date
--
-- Run `dbt build` (see dbt/README.md) to materialize the marts, then
-- refresh these views so Power BI always sees current data.

-- 1) Executive Overview: revenue, orders, average basket, trend
CREATE OR REPLACE VIEW retailsync.gold.daily_sales_bi AS
select
    sales_date,
    count(*) as orders_count,
    sum(line_total) as revenue,
    avg(line_total) as avg_order_value
from retailsync.fact_sales
group by 1;

-- 2) Product Analytics: revenue by product
CREATE OR REPLACE VIEW retailsync.gold.product_sales_bi AS
select
    p.product_id,
    p.name as product_name,
    p.category,
    sum(fs.quantity) as units_sold,
    sum(fs.line_total) as revenue
from retailsync.fact_sales fs
join retailsync.dim_product p on p.product_id = fs.product_id
group by 1, 2, 3;

-- 3) Customer 360: per-customer spend + order count
CREATE OR REPLACE VIEW retailsync.gold.customer_360_bi AS
select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    count(distinct fs.order_id) as orders_count,
    coalesce(sum(fs.line_total), 0) as lifetime_spend,
    coalesce(avg(fs.line_total), 0) as avg_basket
from retailsync.dim_customer c
left join retailsync.fact_sales fs on fs.customer_id = c.customer_id
group by 1, 2, 3, 4;