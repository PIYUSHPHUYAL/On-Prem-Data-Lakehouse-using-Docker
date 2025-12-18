"""
Verify Bronze Layer Data
Read and validate data from MinIO bronze bucket
"""

import os
import io
import pandas as pd
from minio import Minio
from datetime import datetime

def verify_bronze_data(object_name):
    """
    Read data from bronze bucket and display summary
    """
    
    # Initialize MinIO client
    client = Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )
    
    bucket_name = 'bronze'
    
    try:
        # Get object
        print(f"Reading bronze/{object_name}...")
        response = client.get_object(bucket_name, object_name)
        
        # Read into pandas
        df = pd.read_csv(io.BytesIO(response.read()))
        
        print(f"✓ Successfully read data from bronze layer")
        print()
        
        # Display summary
        print("=" * 50)
        print("DATA SUMMARY")
        print("=" * 50)
        print(f"Total records: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print()
        
        print("First 5 records:")
        print(df.head())
        print()
        
        print("Data quality check:")
        print(f"  - Missing amounts: {df['amount'].isna().sum()}")
        print(f"  - Missing cities: {df['city'].isna().sum()}")
        print()
        
        print("Transaction type distribution:")
        print(df['transaction_type'].value_counts())
        
        return True
        
    except Exception as e:
        print(f"✗ Error reading bronze data: {e}")
        return False

def main():
    print("=" * 50)
    print("BRONZE LAYER VERIFICATION")
    print("=" * 50)
    print()
    
    # Use today's date for object path
    today = datetime.now().strftime('%Y-%m-%d')
    object_name = f'transactions/{today}/transactions.csv'
    
    verify_bronze_data(object_name)

if __name__ == "__main__":
    main()
