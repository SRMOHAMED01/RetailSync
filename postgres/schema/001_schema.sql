-- RetailSync OLTP schema (PostgreSQL 16).
--
-- Run once on a fresh database:
--   docker compose up -d postgres
--   docker exec -it retailsync-postgres psql -U retail -d retailsync -f /docker-entrypoint-initdb.d/001_schema.sql
--
-- Logical replication is enabled in docker-compose.yml (wal_level=logical,
-- max_replication_slots=10, max_wal_senders=10) so Debezium can stream the
# WAL. The `REPLICA IDENTITY DEFAULT` (the default for tables with a primary
# key) is sufficient — full row images land in the CDC envelope.

CREATE TABLE IF NOT EXISTS customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL UNIQUE,
    city          TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id    BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    category      TEXT NOT NULL,
    price         NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    stock         INT NOT NULL CHECK (stock >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT NOT NULL REFERENCES customers (customer_id),
    status        TEXT NOT NULL CHECK (status IN (
        'CREATED', 'PAID', 'SHIPPED', 'DELIVERED', 'CANCELLED'
    )),
    total_amount  NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id    BIGINT NOT NULL REFERENCES products (product_id),
    quantity      INT NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id    BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL REFERENCES orders (order_id),
    amount        NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    payment_method TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN (
        'PENDING', 'SUCCESS', 'FAILED', 'REFUNDED'
    )),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed data so the simulator has products to sell and customers to pick.
INSERT INTO customers (first_name, last_name, email, city) VALUES
    ('Sara',   'Alaoui',  'sara@example.com',   'Casablanca'),
    ('Yassine','Amrani',  'yassine@example.com', 'Rabat'),
    ('Mehdi',  'Bennis',  'mehdi@example.com',   'Tanger'),
    ('Leila',  'Chakri',  'leila@example.com',   'Marrakech')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (name, category, price, stock) VALUES
    ('Laptop Pro 15',     'Electronics',  8999, 40),
    ('Mechanical Keyboard','Electronics',   699, 120),
    ('Office Chair',      'Home',         1899, 55),
    ('Wireless Mouse',    'Electronics',   299, 200),
    ('Standing Desk',     'Home',         4500, 12),
    ('USB-C Cable',       'Accessories',    89, 500)
ON CONFLICT DO NOTHING;