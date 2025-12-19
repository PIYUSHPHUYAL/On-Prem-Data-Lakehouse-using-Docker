"""
Silver Layer Transformation
Clean and standardize data from Bronze to Silver
"""

import os
import io
import pandas as pd
from minio import Minio
from datetime import datetime

def read_from_bronze(client, object_name):
    """Read data from bronze bucket"""
    try:
        print(f"Reading from bronze/{object_name}...")
        response = client.get_object('bronze', object_name)
        df = pd.read_csv(io.BytesIO(response.read()))
        print(f"✓ Read {len(df)} records from bronze")
        return df
    except Exception as e:
        print(f"✗ Error reading from bronze: {e}")
        return None

def transform_data(df):
    """Apply transformations and data quality rules"""

    print("\nApplying transformations...")
    initial_count = len(df)

    # 1. Handle missing amounts - remove records with null amounts
    df = df.dropna(subset=['amount'])
    print(f"  - Removed {initial_count - len(df)} records with null amounts")

    # 2. Handle missing cities - fill with 'Unknown'
    null_cities = df['city'].isna().sum()
    df['city'] = df['city'].fillna('Unknown')
    print(f"  - Filled {null_cities} missing cities with 'Unknown'")

    # 3. Ensure correct data types
    df['amount'] = df['amount'].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['transaction_type'] = df['transaction_type'].astype(str).str.lower().str.strip()
    print(f"  - Enforced data types")

    # 4. Add derived columns
    df['transaction_date'] = df['timestamp'].dt.date
    df['transaction_hour'] = df['timestamp'].dt.hour
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    print(f"  - Added derived date columns")

    # 5. Standardize city names
    df['city'] = df['city'].str.title().str.strip()
    print(f"  - Standardized city names")

    print(f"✓ Transformation complete: {len(df)} records")

    return df

def write_to_silver(client, df, object_name):
    """Write transformed data to silver bucket"""
    try:
        print(f"\nWriting to silver/{object_name}...")

        # Convert to CSV
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Upload to silver
        client.put_object(
            'silver',
            object_name,
            csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type='text/csv'
        )

        print(f"✓ Successfully wrote {len(df)} records to silver/{object_name}")
        return True

    except Exception as e:
        print(f"✗ Error writing to silver: {e}")
        return False

def main():
    print("=" * 50)
    print("SILVER LAYER TRANSFORMATION")
    print("=" * 50)
    print()

    # Initialize MinIO client
    client = Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )

    # Auto-detect latest file in bronze bucket
    print("Scanning bronze bucket for latest file...")
    objects = list(client.list_objects('bronze', prefix='transactions/', recursive=True))

    if not objects:
        print("✗ No files found in bronze bucket")
        return

    # Sort by last modified date and get the latest
    latest_object = sorted(objects, key=lambda x: x.last_modified, reverse=True)[0]
    bronze_object = latest_object.object_name

    print(f"✓ Found latest file: {bronze_object}")
    print(f"  Last modified: {latest_object.last_modified}")
    print()

    # Extract date from path for silver output
    # e.g., transactions/2025-12-18/transactions.csv -> 2025-12-18
    date_part = bronze_object.split('/')[1]
    silver_object = f'transactions/{date_part}/transactions_cleaned.csv'

    # ETL Pipeline
    # 1. Extract from Bronze
    df = read_from_bronze(client, bronze_object)
    if df is None:
        print("\n✗ TRANSFORMATION FAILED - Could not read bronze data")
        return

    # 2. Transform
    df_clean = transform_data(df)

    # 3. Load to Silver
    success = write_to_silver(client, df_clean, silver_object)

    print()
    print("=" * 50)
    if success:
        print("✓ SILVER TRANSFORMATION COMPLETE")
    else:
        print("✗ SILVER TRANSFORMATION FAILED")
    print("=" * 50)

if __name__ == "__main__":
    main()
