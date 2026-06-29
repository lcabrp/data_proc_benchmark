# Refactored with Qwen2.5-Coder 7b

import pandas as pd
import numpy as np

# Load benchmark results from a CSV file
df = pd.read_csv('data/benchmark_results.csv')

# Filter the DataFrame for entries related to 'HP-Z2-G9' on Windows platform
hp_df = df[(df['hostname'] == 'HP-Z2-G9') & (df['platform'].str.contains('Windows', na=False))].copy()

# Convert the 'timestamp' column to datetime format
hp_df['timestamp'] = pd.to_datetime(hp_df['timestamp'])

# Split the filtered DataFrame into two batches based on dates
batch_a = hp_df[hp_df['timestamp'].dt.date == pd.to_datetime('2026-05-28').date()].copy()
batch_b = hp_df[hp_df['timestamp'].dt.date == pd.to_datetime('2026-06-01').date()].copy()

# Print the number of runs in each batch
print(f"Batch A runs: {len(batch_a)}")
print(f"Batch B runs: {len(batch_b)}\n")

# Define the operations and libraries to compare
operations = ["filter_group", "statistics", "complex_join", "timeseries"]
libraries = ["pandas", "polars", "duckdb", "fireducks"]

# List to store comparison results
comparison_rows = []

# Iterate over each row in Batch B
for idx, row_b in batch_b.iterrows():
    script_name = row_b['script_name']
    dataset_format = row_b['dataset_format']

    # Find matching rows in Batch A
    row_a_matches = batch_a[(batch_a['script_name'] == script_name) & (batch_a['dataset_format'] == dataset_format)]
    
    if row_a_matches.empty:
        continue
    
    row_a = row_a_matches.iloc[0]

    # Compare each operation and library
    for op in operations:
        for lib in libraries:
            col_name = f"{op}_{lib}_seconds"
            
            if col_name in df.columns:
                val_a = row_a[col_name]
                val_b = row_b[col_name]
                
                # Check prep time too if applicable
                prep_col = f"prep_{lib}_seconds"
                prep_a = row_a.get(prep_col, np.nan)
                prep_b = row_b.get(prep_col, np.nan)
                
                if pd.notna(val_a) or pd.notna(val_b):
                    diff = val_b - val_a if (pd.notna(val_b) and pd.notna(val_a)) else np.nan
                    pct_change = (diff / val_a) * 100 if (pd.notna(diff) and val_a > 0) else np.nan
                    
                    comparison_rows.append({
                        "Script": script_name,
                        "Format": dataset_format,
                        "Operation": op,
                        "Library": lib,
                        "Batch A (May 28)": val_a,
                        "Batch B (June 1)": val_b,
                        "Difference (s)": diff,
                        "Change (%)": pct_change,
                        "Prep A (s)": prep_a,
                        "Prep B (s)": prep_b
                    })

# Create a DataFrame from the comparison rows
comp_df = pd.DataFrame(comparison_rows)

# Print a summary grouped by script, format, and library in a clean markdown table
for (script, fmt), group in comp_df.groupby(["Script", "Format"]):
    print(f"\n======================================================================")
    print(f"SCRIPT: {script} | FORMAT: {fmt.upper()}")
    print(f"======================================================================")
    
    headers = ["Library", "Operation", "Batch A", "Batch B", "Diff (s)", "Change (%)"]
    print(f"| {' | '.join(headers) } |")
    print(f"|{'-'*11}|{'-'*15}|{'-'*11}|{'-'*11}|{'-'*10}|{'-'*12}|")
    
    for _, row in group.iterrows():
        lib = row["Library"]
        op = row["Operation"]
        val_a = f"{row['Batch A (May 28)']:.4f}s" if pd.notna(row['Batch A (May 28)']) else "N/A"
        val_b = f"{row['Batch B (June 1)']:.4f}s" if pd.notna(row['Batch B (June 1)']) else "N/A"
        diff = f"{row['Difference (s)']:.4f}s" if pd.notna(row['Difference (s)']) else "N/A"
        
        pct_change_val = row['Change (%)']
        if pd.isna(pct_change_val):
            pct_change = "N/A"
        else:
            sign = "+" if pct_change_val > 0 else ""
            pct_change = f"{sign}{pct_change_val:.2f}%"
        
        print(f"| {lib:<9} | {op:<13} | {val_a:>9} | {val_b:>9} | {diff:>8} | {pct_change:>10} |")

# Print a summary of preparation times comparison (Batch A vs Batch B)
print("\n\n======================================================================")
print("PREPARATION TIMES COMPARISON (Batch A vs Batch B)")
print("======================================================================")
    
prep_headers = ["Script", "Format", "Library", "Prep Batch A", "Prep Batch B", "Diff (s)"]
print(f"| {' | '.join(prep_headers) } |")
print(f"|{'-'*25}|{'-'*8}|{'-'*11}|{'-'*14}|{'-'*14}|{'-'*10}|")
    
prep_rows_printed = set()
for _, row in comp_df.dropna(subset=["Prep A (s)", "Prep B (s)"]).iterrows():
    key = (row["Script"], row["Format"], row["Library"])
    
    if key in prep_rows_printed:
        continue
    
    prep_rows_printed.add(key)
    
    script, fmt, lib = key
    p_a = f"{row['Prep A (s)']:.4f}s"
    p_b = f"{row['Prep B (s)']:.4f}s"
    diff_val = row['Prep B (s)'] - row['Prep A (s)']
    diff = f"{diff_val:+.4f}s"
    
    print(f"| {script:<23} | {fmt:<6} | {lib:<9} | {p_a:>12} | {p_b:>12} | {diff:>8} |")