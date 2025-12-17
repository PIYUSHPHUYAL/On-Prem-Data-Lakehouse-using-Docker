"""
Infrastructure Test Script
Tests connectivity to MinIO and PostgreSQL
"""

import os
import sys

def test_minio_connection():
    """Test MinIO connectivity and bucket creation"""
    try:
        from minio import Minio
        
        client = Minio(
            os.getenv('MINIO_ENDPOINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False
        )
        
        # List buckets
        buckets = client.list_buckets()
        bucket_names = [b.name for b in buckets]
        
        print("✓ MinIO Connection: SUCCESS")
        print(f"  Buckets found: {bucket_names}")
        
        # Verify medallion buckets exist
        required_buckets = ['bronze', 'silver', 'gold']
        missing = [b for b in required_buckets if b not in bucket_names]
        
        if missing:
            print(f"  ✗ Missing buckets: {missing}")
            return False
        
        print("  ✓ All medallion buckets present")
        return True
        
    except Exception as e:
        print(f"✗ MinIO Connection: FAILED")
        print(f"  Error: {e}")
        return False

def test_postgres_connection():
    """Test PostgreSQL connectivity"""
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DB')
        )
        
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        print("✓ PostgreSQL Connection: SUCCESS")
        print(f"  Version: {version.split(',')[0]}")
        
        # Check if init script ran
        cursor.execute("SELECT COUNT(*) FROM pipeline_metadata;")
        count = cursor.fetchone()[0]
        print(f"  ✓ Init script executed (metadata table has {count} record)")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ PostgreSQL Connection: FAILED")
        print(f"  Error: {e}")
        return False

def main():
    """Run all infrastructure tests"""
    print("=" * 50)
    print("INFRASTRUCTURE TEST")
    print("=" * 50)
    print()
    
    minio_ok = test_minio_connection()
    print()
    postgres_ok = test_postgres_connection()
    print()
    
    print("=" * 50)
    if minio_ok and postgres_ok:
        print("✓ ALL TESTS PASSED - Infrastructure is ready!")
        print("=" * 50)
        sys.exit(0)
    else:
        print("✗ SOME TESTS FAILED - Check logs above")
        print("=" * 50)
        sys.exit(1)

if __name__ == "__main__":
    main()
