"""
Load Gold Layer data into PostgreSQL
Serving layer for BI tools and applications
"""

import os
import io
import pandas as pd
import psycopg2
from minio import Minio

def get_postgres_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DB')
        )
        print("✓ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")
        return None

def read_gold_dataset(client, dataset_name):
    """Read dataset from gold bucket"""
    try:
        # Find latest file
        objects = list(client.list_objects('gold', prefix=f'{dataset_name}/', recursive=True))
        if not objects:
            print(f"✗ No files found for {dataset_name}")
            return None
        
        latest = sorted(objects, key=lambda x: x.last_modified, reverse=True)[0]
        
        print(f"  Reading {latest.object_name}...")
        response = client.get_object('gold', latest.object_name)
        df = pd.read_csv(io.BytesIO(response.read()))
        print(f"  ✓ Loaded {len(df)} records")
        return df
        
    except Exception as e:
        print(f"✗ Error reading {dataset_name}: {e}")
        return None

def load_to_postgres(df, table_name, conn):
    """Load dataframe into PostgreSQL table"""
    try:
        cursor = conn.cursor()
        
        # Drop table if exists and create new
        cursor.execute(f"DROP TABLE IF EXISTS gold.{table_name} CASCADE;")
        
        # Create table from dataframe
        columns = []
        for col in df.columns:
            dtype = df[col].dtype
            if dtype == 'object':
                sql_type = 'TEXT'
            elif dtype == 'int64':
                sql_type = 'INTEGER'
            elif dtype == 'float64':
                sql_type = 'NUMERIC'
            else:
                sql_type = 'TEXT'
            columns.append(f'"{col}" {sql_type}')
        
        create_sql = f"""
        CREATE TABLE gold.{table_name} (
            {', '.join(columns)}
        );
        """
        cursor.execute(create_sql)
        
        # Insert data
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            insert_sql = f'INSERT INTO gold.{table_name} VALUES ({placeholders})'
            cursor.execute(insert_sql, tuple(row))
        
        conn.commit()
        cursor.close()
        
        print(f"  ✓ Loaded {len(df)} records into gold.{table_name}")
        return True
        
    except Exception as e:
        print(f"✗ Error loading to PostgreSQL: {e}")
        conn.rollback()
        return False

def main():
    print("=" * 50)
    print("LOAD GOLD DATA TO POSTGRESQL")
    print("=" * 50)
    print()
    
    # Initialize clients
    minio_client = Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )
    
    pg_conn = get_postgres_connection()
    if not pg_conn:
        return
    
    # Datasets to load
    datasets = [
        ('daily_summary', 'daily_summary'),
        ('city_summary', 'city_summary'),
        ('transaction_type_summary', 'transaction_type_summary'),
        ('hourly_pattern', 'hourly_pattern')
    ]
    
    results = []
    for dataset_name, table_name in datasets:
        print(f"\nProcessing {dataset_name}...")
        df = read_gold_dataset(minio_client, dataset_name)
        if df is not None:
            success = load_to_postgres(df, table_name, pg_conn)
            results.append(success)
        else:
            results.append(False)
    
    pg_conn.close()
    
    print()
    print("=" * 50)
    if all(results):
        print(f"✓ ALL {len(datasets)} DATASETS LOADED TO POSTGRESQL")
    else:
        print("✗ SOME DATASETS FAILED TO LOAD")
    print("=" * 50)

if __name__ == "__main__":
    main()
