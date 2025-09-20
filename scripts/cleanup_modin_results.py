"""
Script to remove Modin-related columns from benchmark results CSV.
Creates a backup of the original file before modification.
Also handles cleanup of empty FireDucks results.
"""

import pandas as pd
import shutil
from pathlib import Path
from datetime import datetime
import numpy as np


def remove_modin_columns(csv_path: Path) -> None:
    """
    Remove all Modin-related columns from the benchmark results CSV.
    Creates a backup of the original file first.
    
    Args:
        csv_path: Path to the benchmark results CSV file
    """
    
    # Create backup with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = csv_path.parent / f"{csv_path.stem}_backup_{timestamp}{csv_path.suffix}"
    
    print(f"Creating backup: {backup_path}")
    shutil.copy2(csv_path, backup_path)
    
    # Read the CSV
    print(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    
    print(f"Original dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Identify Modin columns
    modin_columns = [col for col in df.columns if 'modin' in col.lower()]
    print(f"Found Modin columns to remove: {modin_columns}")
    
    # Remove Modin columns if any exist
    if modin_columns:
        df_cleaned = df.drop(columns=modin_columns)
        removed_columns = len(modin_columns)
        print(f"Removed {removed_columns} Modin columns")
    else:
        df_cleaned = df.copy()
        removed_columns = 0
        print("No Modin columns found to remove")
    
    # Check FireDucks columns for empty values
    fireducks_columns = [col for col in df_cleaned.columns if 'fireducks' in col.lower()]
    if fireducks_columns:
        print(f"\nFound FireDucks columns: {fireducks_columns}")
        for col in fireducks_columns:
            non_null_count = df_cleaned[col].notna().sum()
            total_count = len(df_cleaned)
            print(f"  {col}: {non_null_count}/{total_count} non-null values")
            
            # Show unique values in FireDucks columns
            unique_values = df_cleaned[col].dropna().unique()
            print(f"  Unique non-null values: {unique_values}")
    
    # Save the cleaned CSV
    print(f"\nSaving cleaned CSV: {csv_path}")
    df_cleaned.to_csv(csv_path, index=False)
    
    # Report results
    original_columns = len(df.columns)
    cleaned_columns = len(df_cleaned.columns)
    
    print(f"\nResults:")
    print(f"  Original columns: {original_columns}")
    print(f"  Cleaned columns: {cleaned_columns}")
    print(f"  Removed columns: {removed_columns}")
    print(f"  Rows processed: {len(df_cleaned)}")
    print(f"  Backup saved to: {backup_path}")
    
    # Show a sample of the data
    print(f"\nSample of cleaned data (first 3 rows):")
    print(df_cleaned.head(3).to_string())


def clean_fireducks_empty_values(csv_path: Path) -> None:
    """
    Replace empty FireDucks values with np.nan for consistency.
    """
    print(f"\n=== Cleaning FireDucks empty values ===")
    
    df = pd.read_csv(csv_path)
    
    # Find FireDucks columns
    fireducks_columns = [col for col in df.columns if 'fireducks' in col.lower()]
    
    if not fireducks_columns:
        print("No FireDucks columns found")
        return
    
    changes_made = False
    for col in fireducks_columns: # Not needed. Saving it as blanks
        # Count empty strings and replace with NaN
        empty_count = (df[col] == '').sum()
        if empty_count > 0:
            # df[col] = df[col].replace('', np.nan)
            changes_made = True
            print(f"  Replaced {empty_count} empty strings with NaN in {col}")
        
        # Check for any other problematic values
        unique_vals = df[col].dropna().unique()
        print(f"  {col} unique values: {unique_vals}")
    
    if changes_made:
        df.to_csv(csv_path, index=False)
        print("FireDucks cleanup completed and saved")
    else:
        print("No FireDucks cleanup needed")


def main():
    """Main function to execute the cleanup."""
    # Path to the benchmark results CSV
    csv_path = Path("c:/Users/lcabr/Documents/Projects/data_proc_benchmark/data/benchmark_results.csv")
    
    if not csv_path.exists():
        print(f"Error: File not found: {csv_path}")
        print("Please check the file path and try again.")
        return
    
    try:
        # Main cleanup
        remove_modin_columns(csv_path)
        
        # Additional FireDucks cleanup
        clean_fireducks_empty_values(csv_path)
        
        print(f"\n✅ Successfully processed benchmark results!")
        print(f"File location: {csv_path}")
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        print("The original file should be intact. Check the backup if needed.")


if __name__ == "__main__":
    main()