"""
Master Pipeline Orchestrator
Run the entire data lakehouse pipeline end-to-end
"""

import subprocess
import sys
from datetime import datetime

def run_script(script_name, description):
    """Run a Python script and handle errors"""
    print("\n" + "=" * 60)
    print(f"RUNNING: {description}")
    print("=" * 60)
    
    try:
        result = subprocess.run(
            ['python', f'scripts/{script_name}'],
            check=True,
            capture_output=False
        )
        print(f"✓ {description} - SUCCESS")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} - FAILED")
        print(f"Error: {e}")
        return False

def main():
    start_time = datetime.now()
    
    print("=" * 60)
    print("DATA LAKEHOUSE PIPELINE")
    print("End-to-End Execution")
    print("=" * 60)
    print(f"Started at: {start_time}")
    print()
    
    # Pipeline stages
    stages = [
        ('generate_data.py', 'Data Generation'),
        ('ingest_bronze.py', 'Bronze Layer Ingestion'),
        ('transform_silver.py', 'Silver Layer Transformation'),
        ('curate_gold.py', 'Gold Layer Curation'),
        ('analytics_duckdb.py', 'DuckDB Analytics'),
        ('load_postgres.py', 'PostgreSQL Serving Layer')
    ]
    
    results = []
    for script, description in stages:
        success = run_script(script, description)
        results.append((description, success))
        if not success:
            print(f"\n✗ Pipeline failed at: {description}")
            print("Fix the error and run again.")
            sys.exit(1)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    
    for stage, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status} - {stage}")
    
    print()
    print(f"Total execution time: {duration:.2f} seconds")
    print(f"Completed at: {end_time}")
    print("=" * 60)
    print("✓ PIPELINE COMPLETE - All layers ready!")
    print("=" * 60)

if __name__ == "__main__":
    main()
