"""
Gold Layer Curation
Create analytics-ready aggregated datasets from Silver
"""

import os
import io
import pandas as pd
from minio import Minio
from datetime import datetime

def read_from_silver(client):
    """Read latest cleaned data from silver bucket"""
    try:
        print("Scanning silver bucket for latest file...")
        objects = list(client.list_objects('silver', prefix='transactions/', recursive=True))
        
        if not objects:
            print("✗ No files found in silver bucket")
            return None
        
        latest_object = sorted(objects, key=lambda x: x.last_modified, reverse=True)[0]
        silver_object = latest_object.object_name
        
        print(f"✓ Reading from silver/{silver_object}")
        response = client.get_object('silver', silver_object)
        df = pd.read_csv(io.BytesIO(response.read()))
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        print(f"✓ Loaded {len(df)} records from silver")
        return df
        
    except Exception as e:
        print(f"✗ Error reading from silver: {e}")
        return None

def create_daily_summary(df):
    """Aggregate: Daily transaction summary"""
    print("\nCreating daily summary...")
    
    daily = df.groupby('transaction_date').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean', 'min', 'max'],
        'account_id': 'nunique'
    }).reset_index()
    
    # Flatten column names
    daily.columns = [
        'transaction_date',
        'total_transactions',
        'total_amount',
        'avg_amount',
        'min_amount',
        'max_amount',
        'unique_accounts'
    ]
    
    print(f"✓ Created daily summary: {len(daily)} days")
    return daily

def create_city_summary(df):
    """Aggregate: City-level transaction summary"""
    print("Creating city summary...")
    
    city = df.groupby('city').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean'],
        'account_id': 'nunique'
    }).reset_index()
    
    city.columns = [
        'city',
        'total_transactions',
        'total_amount',
        'avg_amount',
        'unique_accounts'
    ]
    
    # Sort by total amount
    city = city.sort_values('total_amount', ascending=False)
    
    print(f"✓ Created city summary: {len(city)} cities")
    return city

def create_transaction_type_summary(df):
    """Aggregate: Transaction type analysis"""
    print("Creating transaction type summary...")
    
    txn_type = df.groupby('transaction_type').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean', 'median']
    }).reset_index()
    
    txn_type.columns = [
        'transaction_type',
        'total_transactions',
        'total_amount',
        'avg_amount',
        'median_amount'
    ]
    
    print(f"✓ Created transaction type summary: {len(txn_type)} types")
    return txn_type

def create_hourly_pattern(df):
    """Aggregate: Hourly transaction patterns"""
    print("Creating hourly pattern analysis...")
    
    hourly = df.groupby('transaction_hour').agg({
        'transaction_id': 'count',
        'amount': 'sum'
    }).reset_index()
    
    hourly.columns = [
        'hour',
        'total_transactions',
        'total_amount'
    ]
    
    print(f"✓ Created hourly pattern: {len(hourly)} hours")
    return hourly

def write_to_gold(client, df, dataset_name):
    """Write curated dataset to gold bucket"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        object_name = f'{dataset_name}/{today}/{dataset_name}.csv'
        
        print(f"  Writing to gold/{object_name}...")
        
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        client.put_object(
            'gold',
            object_name,
            csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type='text/csv'
        )
        
        print(f"  ✓ Wrote {len(df)} records")
        return True
        
    except Exception as e:
        print(f"  ✗ Error writing to gold: {e}")
        return False

def main():
    print("=" * 50)
    print("GOLD LAYER CURATION")
    print("=" * 50)
    print()
    
    # Initialize MinIO client
    client = Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )
    
    # Read from Silver
    df = read_from_silver(client)
    if df is None:
        print("\n✗ CURATION FAILED - Could not read silver data")
        return
    
    # Create aggregated datasets
    daily_summary = create_daily_summary(df)
    city_summary = create_city_summary(df)
    txn_type_summary = create_transaction_type_summary(df)
    hourly_pattern = create_hourly_pattern(df)
    
    # Write to Gold
    print("\nWriting curated datasets to gold layer...")
    results = []
    results.append(write_to_gold(client, daily_summary, 'daily_summary'))
    results.append(write_to_gold(client, city_summary, 'city_summary'))
    results.append(write_to_gold(client, txn_type_summary, 'transaction_type_summary'))
    results.append(write_to_gold(client, hourly_pattern, 'hourly_pattern'))
    
    print()
    print("=" * 50)
    if all(results):
        print("✓ GOLD CURATION COMPLETE")
        print(f"  - 4 datasets created")
    else:
        print("✗ SOME GOLD DATASETS FAILED")
    print("=" * 50)

if __name__ == "__main__":
    main()
