-- ============================================================
-- ScyllaDB Schema Initialization
-- ============================================================

-- Create keyspace with SimpleStrategy for local dev
CREATE KEYSPACE IF NOT EXISTS transactions_ks
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

-- Use the keyspace
USE transactions_ks;

-- ─── Table: daily_customer_totals ──────────────────────────
-- Partition key : customer_id  (groups all dates for one customer)
-- Clustering key: transaction_date (sorted order within partition)
-- daily_total   : aggregated sum of valid transactions
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_customer_totals (
    customer_id      TEXT,
    transaction_date DATE,
    daily_total      FLOAT,
    PRIMARY KEY (customer_id, transaction_date)
)
WITH CLUSTERING ORDER BY (transaction_date DESC)
AND  comment = 'Daily aggregated transaction totals per customer';

-- ─── Optional: raw_transactions for audit/debugging ────────
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id   TEXT,
    customer_id      TEXT,
    amount           FLOAT,
    ts               TIMESTAMP,
    merchant         TEXT,
    PRIMARY KEY (transaction_id)
);

-- Verify
DESCRIBE TABLES;
