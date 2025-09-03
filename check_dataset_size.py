#!/usr/bin/env python3
"""Check the dataset size and add dataset_size column to existing CSV."""

import pandas as pd
import csv
from pathlib import Path

# Paths
CSV_PATH = Path("data/raw/synthetic_logs_test.csv")
RESULTS_CSV_PATH = Path("data/benchmark_results.csv")

def main():
    # Get dataset size
    try:
        df = pd.read_csv(CSV_PATH)
        dataset_size = len(df)
        print(f"Dataset size: {dataset_size:,} records")
        
        # Since you mentioned you've been using 10,000,000 records,
        # let's verify this matches
        if dataset_size == 10000000:
            print("✓ Matches expected 10M records")
        else:
            print(f"⚠ Different from expected 10M records (actual: {dataset_size:,})")
            
    except Exception as e:
        print(f"Error reading dataset: {e}")
        dataset_size = 10000000  # Default based on your statement
        print(f"Using default dataset size: {dataset_size:,}")

    # Read existing CSV and add dataset_size column
    try:
        # Read existing results
        results_df = pd.read_csv(RESULTS_CSV_PATH)
        
        # Add dataset_size column (assuming all existing records used the same dataset)
        results_df['dataset_size'] = dataset_size
        
        # Reorder columns to put dataset_size after cpu_arch and before timing columns
        columns = list(results_df.columns)
        
        # Find the position to insert dataset_size (after cpu_arch)
        if 'cpu_arch' in columns:
            cpu_arch_idx = columns.index('cpu_arch')
            # Remove dataset_size from its current position
            columns.remove('dataset_size')
            # Insert it after cpu_arch
            columns.insert(cpu_arch_idx + 1, 'dataset_size')
        else:
            # If cpu_arch not found, put it before the first timing column
            timing_cols = [col for col in columns if '_seconds' in col]
            if timing_cols:
                first_timing_idx = columns.index(timing_cols[0])
                columns.remove('dataset_size')
                columns.insert(first_timing_idx, 'dataset_size')
        
        # Reorder DataFrame columns
        results_df = results_df[columns]
        
        # Save updated CSV
        results_df.to_csv(RESULTS_CSV_PATH, index=False)
        print(f"✓ Updated {RESULTS_CSV_PATH} with dataset_size column")
        print(f"✓ Added dataset_size = {dataset_size:,} to {len(results_df)} existing records")
        
    except Exception as e:
        print(f"Error updating CSV: {e}")

if __name__ == "__main__":
    main()
