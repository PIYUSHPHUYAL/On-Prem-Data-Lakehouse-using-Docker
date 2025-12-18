"""
Bronze Layer Ingestion
Load raw CSV data into MinIO bronze bucket
"""

import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error

def ingest_to_bronze(local_file_path, object_name):
    """
    Upload raw data file to MinIO bronze bucket
    
    Args:
        local_file_path: Path to local CSV file
        object_name: Name for object in MinIO (e.g., 'transactions/2024-12-16/data.csv')
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
        # Check if bucket exists
        if not client.bucket_exists(bucket_name):
            print(f"✗ Bucket '{bucket_name}' does not exist")
            return False
        
        # Upload file
        print(f"Uploading {local_file_path} to bronze/{object_name}...")
        
        client.fput_object(
            bucket_name,
            object_name,
            local_file_path,
        )
        
        print(f"✓ Successfully uploaded to bronze/{object_name}")
        
        # Get file stats
        stat = client.stat_object(bucket_name, object_name)
        print(f"  Size: {stat.size / 1024:.2f} KB")
        print(f"  Last Modified: {stat.last_modified}")
        
        return True
        
    except S3Error as e:
        print(f"✗ MinIO error: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    print("=" * 50)
    print("BRONZE LAYER INGESTION")
    print("=" * 50)
    print()
    
    # Define file paths
    local_file = 'data/raw/transactions.csv'
    
    # Create object name with date partition
    today = datetime.now().strftime('%Y-%m-%d')
    object_name = f'transactions/{today}/transactions.csv'
    
    # Ingest to bronze
    success = ingest_to_bronze(local_file, object_name)
    
    print()
    print("=" * 50)
    if success:
        print("✓ BRONZE INGESTION COMPLETE")
    else:
        print("✗ BRONZE INGESTION FAILED")
    print("=" * 50)

if __name__ == "__main__":
    main()
