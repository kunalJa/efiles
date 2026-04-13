#!/usr/bin/env python3
"""
Prepare S3 Inventory CSV for DynamoDB bulk import.

Takes the raw S3 inventory (no headers) and:
1. Filters to only PDF files
2. Prioritizes first 500 rows: 250 from VOL00009, 250 from VOL00010 (shuffled)
3. Shuffles the remaining files randomly
4. Adds sequential ID and Status columns
5. Outputs CSV ready for DynamoDB import

Usage:
    python prepare_dynamo_import.py
"""

import pandas as pd
from pathlib import Path

# Paths
INPUT_CSV = "/mnt/c/Users/kcube/Desktop/E_Files/s3_inventory.csv"
OUTPUT_CSV = "/mnt/c/Users/kcube/Desktop/E_Files/shuffled_dynamo_import.csv"

def main():
    print("Loading S3 Inventory...")
    
    # Load the raw S3 Inventory CSV (no headers)
    # Columns: Bucket, S3Key, Size, LastModified
    df = pd.read_csv(
        INPUT_CSV,
        header=None,
        names=['Bucket', 'S3Key', 'Size', 'LastModified']
    )
    
    # We only care about the S3 key (file path)
    df = df[['S3Key']]
    
    # Filter to only PDF files
    df = df[df['S3Key'].str.lower().str.endswith('.pdf')]
    
    print(f"Found {len(df):,} PDFs. Shuffling...")
    
    # 1. Grab 250 random files from Volume 9
    vol_9_pool = df[df['S3Key'].str.contains('VOL00009')]
    vol_9_count = min(250, len(vol_9_pool))
    first_vol9 = vol_9_pool.sample(n=vol_9_count)
    print(f"  Selected {vol_9_count} from VOL00009 (pool: {len(vol_9_pool)})")
    
    # 2. Grab 250 random files from Volume 10
    vol_10_pool = df[df['S3Key'].str.contains('VOL00010')]
    vol_10_count = min(250, len(vol_10_pool))
    first_vol10 = vol_10_pool.sample(n=vol_10_count)
    print(f"  Selected {vol_10_count} from VOL00010 (pool: {len(vol_10_pool)})")
    
    # 3. Combine and shuffle the top 500 so they're evenly mixed
    top_500 = pd.concat([first_vol9, first_vol10]).sample(frac=1)
    print(f"  Top {len(top_500)} items shuffled (VOL9 + VOL10)")
    
    # 4. Get the rest of the files (excluding the ones we picked)
    the_rest = df.drop(top_500.index)
    
    # 5. Shuffle the remaining files
    shuffled_rest = the_rest.sample(frac=1)
    print(f"  Shuffled remaining {len(shuffled_rest):,} items")
    
    # 6. Stack: Top 500 first, then the shuffled rest
    final_df = pd.concat([top_500, shuffled_rest]).reset_index(drop=True)
    
    # Add DynamoDB schema columns
    final_df['ID'] = range(1, len(final_df) + 1)
    final_df['Status'] = 'AVAILABLE'
    
    # Reorder columns for DynamoDB: ID, S3Key, Status
    final_df = final_df[['ID', 'S3Key', 'Status']]
    
    # Save the shuffled CSV with headers (DynamoDB needs headers)
    final_df.to_csv(OUTPUT_CSV, index=False)
    
    print(f"\nDone!")
    print(f"  Total items: {len(final_df):,}")
    print(f"  Output: {OUTPUT_CSV}")
    print(f"\nNext steps:")
    print(f"  1. Upload {Path(OUTPUT_CSV).name} to your S3 bucket")
    print(f"  2. Go to DynamoDB Console > Imports from S3")
    print(f"  3. Select CSV, check 'First row is header'")
    print(f"  4. Set partition key: ID (Number)")

if __name__ == "__main__":
    main()
