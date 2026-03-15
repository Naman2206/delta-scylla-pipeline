#!/usr/bin/env python3
"""Insert sample data into ScyllaDB daily_customer_totals table."""

import argparse
import random
from datetime import date, timedelta

from cassandra.cluster import Cluster

KEYSPACE = "transactions_ks"
TABLE = "daily_customer_totals"


def create_keyspace_and_table(session):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """
    )

    session.execute(f"USE {KEYSPACE};")

    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            customer_id TEXT,
            transaction_date DATE,
            daily_total FLOAT,
            PRIMARY KEY (customer_id, transaction_date)
        ) WITH CLUSTERING ORDER BY (transaction_date DESC);
        """
    )


def generate_sample_data(num_customers=1000, days=7):
    """Generate sample daily totals for a set of customers."""
    base_date = date.today()
    rows = []

    for ci in range(1, num_customers + 1):
        customer_id = f"C{ci:05d}"
        for d in range(days):
            tx_date = base_date - timedelta(days=d)
            total = round(random.uniform(50, 1500), 2)
            rows.append((customer_id, tx_date, float(total)))

    return rows


def insert_rows(session, rows, batch_size=500):
    stmt = session.prepare(
        f"INSERT INTO {KEYSPACE}.{TABLE} (customer_id, transaction_date, daily_total) VALUES (?, ?, ?)"
    )

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        for row in batch:
            session.execute(stmt, row)


def main():
    parser = argparse.ArgumentParser(description="Insert sample data into ScyllaDB")
    parser.add_argument("--customers", type=int, default=1000, help="Number of customers to generate")
    parser.add_argument("--days", type=int, default=7, help="Number of days of data per customer")
    args = parser.parse_args()

    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect()

    create_keyspace_and_table(session)

    rows = generate_sample_data(num_customers=args.customers, days=args.days)
    insert_rows(session, rows)

    print(f"Inserted {len(rows)} rows into {KEYSPACE}.{TABLE}")

    # Show sample
    result = session.execute(f"SELECT * FROM {KEYSPACE}.{TABLE} LIMIT 10")
    for r in result:
        print(r)

    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    main()
