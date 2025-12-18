"""
Generate realistic bank transaction data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_transactions(num_records=10000):
    """Generate realistic transaction data"""
    
    # Set seed for reproducibility
    np.random.seed(42)
    random.seed(42)
    
    # Define data parameters
    cities = ['Kathmandu', 'Pokhara', 'Lalitpur', 'Biratnagar', 'Bhaktapur', 
              'Dharan', 'Butwal', 'Hetauda', 'Janakpur', 'Nepalgunj']
    
    transaction_types = ['deposit', 'withdrawal', 'transfer', 'payment']
    
    # Generate base data
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    data = {
        'transaction_id': [f'TXN{str(i).zfill(8)}' for i in range(1, num_records + 1)],
        'account_id': [f'ACC{str(random.randint(1000, 9999)).zfill(4)}' for _ in range(num_records)],
        'amount': np.round(np.random.lognormal(mean=8, sigma=2, size=num_records), 2),
        'transaction_type': np.random.choice(transaction_types, size=num_records, 
                                            p=[0.3, 0.25, 0.25, 0.2]),
        'timestamp': [start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        ) for _ in range(num_records)],
        'city': np.random.choice(cities, size=num_records)
    }
    
    df = pd.DataFrame(data)
    
    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Add some data quality issues (realistic for bronze layer)
    # 1. Some null amounts (rare)
    null_indices = np.random.choice(df.index, size=int(num_records * 0.01), replace=False)
    df.loc[null_indices, 'amount'] = np.nan
    
    # 2. Some records with missing city
    null_city_indices = np.random.choice(df.index, size=int(num_records * 0.02), replace=False)
    df.loc[null_city_indices, 'city'] = None
    
    return df

def main():
    print("Generating bank transaction data...")
    
    # Generate data
    df = generate_transactions(10000)
    
    # Save to CSV
    output_path = 'data/raw/transactions.csv'
    df.to_csv(output_path, index=False)
    
    print(f"✓ Generated {len(df)} transactions")
    print(f"✓ Saved to: {output_path}")
    print(f"\nData preview:")
    print(df.head(10))
    print(f"\nData info:")
    print(df.info())

if __name__ == "__main__":
    main()
