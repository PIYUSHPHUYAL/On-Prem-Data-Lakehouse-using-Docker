"""
DuckDB Analytics on Gold Layer
Query Gold data directly from MinIO using DuckDB
"""

import os
import duckdb
from minio import Minio

def setup_duckdb_with_minio():
    """Configure DuckDB to read from MinIO"""

    conn = duckdb.connect(':memory:')

    # Install and load httpfs extension for S3 access
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # Configure S3 settings for MinIO
    conn.execute(f"""
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='{os.getenv('MINIO_ACCESS_KEY')}';
        SET s3_secret_access_key='{os.getenv('MINIO_SECRET_KEY')}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    print("✓ DuckDB configured for MinIO access")
    return conn

def query_daily_summary(conn):
    """Query 1: Top 10 days by transaction volume"""
    print("\n" + "=" * 50)
    print("QUERY 1: Top 10 Days by Transaction Volume")
    print("=" * 50)

    query = """
    SELECT
        transaction_date,
        total_transactions,
        total_amount,
        avg_amount,
        unique_accounts
    FROM read_csv_auto('s3://gold/daily_summary/*/daily_summary.csv')
    ORDER BY total_transactions DESC
    LIMIT 10;
    """

    result = conn.execute(query).fetchdf()
    print(result.to_string(index=False))
    return result

def query_city_rankings(conn):
    """Query 2: City performance rankings"""
    print("\n" + "=" * 50)
    print("QUERY 2: Top Cities by Transaction Volume")
    print("=" * 50)

    query = """
    SELECT
        city,
        total_transactions,
        ROUND(total_amount, 2) as total_amount,
        ROUND(avg_amount, 2) as avg_amount,
        unique_accounts
    FROM read_csv_auto('s3://gold/city_summary/*/city_summary.csv')
    ORDER BY total_amount DESC;
    """

    result = conn.execute(query).fetchdf()
    print(result.to_string(index=False))
    return result

def query_transaction_types(conn):
    """Query 3: Transaction type breakdown"""
    print("\n" + "=" * 50)
    print("QUERY 3: Transaction Type Analysis")
    print("=" * 50)

    query = """
    SELECT
        transaction_type,
        total_transactions,
        ROUND(total_amount, 2) as total_amount,
        ROUND(avg_amount, 2) as avg_amount,
        ROUND(median_amount, 2) as median_amount
    FROM read_csv_auto('s3://gold/transaction_type_summary/*/transaction_type_summary.csv')
    ORDER BY total_amount DESC;
    """

    result = conn.execute(query).fetchdf()
    print(result.to_string(index=False))
    return result

def query_peak_hours(conn):
    """Query 4: Peak transaction hours"""
    print("\n" + "=" * 50)
    print("QUERY 4: Peak Transaction Hours")
    print("=" * 50)

    query = """
    SELECT
        hour,
        total_transactions,
        ROUND(total_amount, 2) as total_amount
    FROM read_csv_auto('s3://gold/hourly_pattern/*/hourly_pattern.csv')
    ORDER BY total_transactions DESC
    LIMIT 10;
    """

    result = conn.execute(query).fetchdf()
    print(result.to_string(index=False))
    return result

def main():
    print("=" * 50)
    print("DUCKDB ANALYTICS ON GOLD LAYER")
    print("=" * 50)

    # Setup DuckDB
    conn = setup_duckdb_with_minio()

    # Run analytics queries
    query_daily_summary(conn)
    query_city_rankings(conn)
    query_transaction_types(conn)
    query_peak_hours(conn)

    print("\n" + "=" * 50)
    print("✓ ANALYTICS COMPLETE")
    print("=" * 50)

    conn.close()

if __name__ == "__main__":
    main()
