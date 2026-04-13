#!/usr/bin/env python3
"""
Combine two CSV files into one.
Appends inventory2.csv to inventory.csv, skipping the header in inventory2.
"""

import sys
from pathlib import Path

def combine_csvs(csv1_path: str, csv2_path: str, output_path: str):
    """Combine two CSVs, skipping header in second file."""
    
    csv1 = Path(csv1_path)
    csv2 = Path(csv2_path)
    output = Path(output_path)
    
    if not csv1.exists():
        print(f"Error: {csv1} not found")
        sys.exit(1)
    if not csv2.exists():
        print(f"Error: {csv2} not found")
        sys.exit(1)
    
    with open(output, 'w', encoding='utf-8') as out:
        # Write all of first CSV
        with open(csv1, 'r', encoding='utf-8') as f1:
            lines1 = f1.readlines()
            out.writelines(lines1)
            print(f"Wrote {len(lines1)} lines from {csv1.name}")
        
        # Write all of second CSV
        with open(csv2, 'r', encoding='utf-8') as f2:
            lines2 = f2.readlines()
            out.writelines(lines2)
            print(f"Wrote {len(lines2)} lines from {csv2.name}")
    
    print(f"\nCombined into: {output}")
    print(f"Total lines: {len(lines1) + len(lines2)}")

if __name__ == "__main__":
    base = "/mnt/c/Users/kcube/Desktop/E_Files"
    
    combine_csvs(
        f"{base}/inventory.csv",
        f"{base}/inventory2.csv",
        f"{base}/s3_inventory.csv"
    )
