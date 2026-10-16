# Power BI — RetailSync Dashboard (importable spec)

Power BI `.pbix` files are binary and cannot be committed to Git. Use this spec
to build the report in Power BI Desktop against the Databricks SQL Warehouse
(see `powerbi/README.md`).

## Data source
- Type: **Databricks SQL** (ODBC / DirectQuery recommended for freshness)
- Warehouse: the SQL Warehouse created for dbt + Power BI
- Gold views to consume (in `retailsync.gold`):
  - `daily_sales_bi` — Executive Overview
  - `product_sales_bi` — Product Analytics
  - `customer_360_bi` — Customer 360
- Alternatively query the dbt Gold models directly: `ref('daily_sales')`,
  `ref('fact_sales')`, `ref('dim_date')`, `ref('dim_product')`, `ref('dim_customer')`,
  `ref('fact_orders')`.

## Model
| Table | Key | Relationship |
|---|---|---|
| `daily_sales_bi` | `sales_date` | many-to-one → `dim_date.date_id` |
| `fact_sales` | `order_item_id` | many-to-one → `dim_product.product_id`, `dim_customer.customer_id` |
| `fact_orders` | `order_id` | many-to-one → `dim_customer.customer_id` |
| `dim_date` | `date_id` | date spine |
| `dim_product` | `product_id` | product dimension |
| `dim_customer` | `customer_id` | customer dimension |

## DAX measures
```dax
Revenue = SUM(fact_sales[line_total])
Orders = DISTINCTCOUNT(fact_orders[order_id])
Avg Basket = DIVIDE([Revenue], [Orders])
Customers = DISTINCTCOUNT(dim_customer[customer_id])
Units Sold = SUM(fact_sales[quantity])
```

## Pages

### Page 1 — Executive Overview
- **KPI cards**: Revenue, Orders, Avg Basket, Customers
- **Line chart**: Revenue by `sales_date` (trend)
- **Slicer**: `dim_date.year`

### Page 2 — Product Analytics
- **Table**: `product_sales_bi` (product_name, category, units_sold, revenue)
- **Bar chart**: Top 10 products by revenue
- **Pie chart**: Revenue share by `category`

### Page 3 — Customer 360
- **Table**: `customer_360_bi` (customer_id, city, orders_count, lifetime_spend, avg_basket)
- **Bar chart**: Top 10 customers by `lifetime_spend`
- **Map**: customers by `city` (if geography enabled)

## Refresh
- DirectQuery: near-real-time (no scheduled refresh needed)
- Import mode: schedule daily; Gold/Silver freshness is driven by CDC + dbt

## Security
- Do **not** connect Power BI directly to PostgreSQL OLTP for this project.
- Apply row-level security on `dim_customer` if multi-tenant.