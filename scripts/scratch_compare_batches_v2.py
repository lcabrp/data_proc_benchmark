def load_data(file_path):
    """Load benchmark results from a CSV file."""
    return pd.read_csv(file_path)

def filter_data(df):
    """Filter data for HP-Z2-G9 on Windows."""
    return df[(df['hostname'] == 'HP-Z2-G9') & (df['platform'].str.contains('Windows', na=False))].copy()

def split_batches(filtered_df, date_batch_a='2026-05-28', date_batch_b='2026-06-01'):
    """Split filtered data into Batch A and Batch B."""
    batch_a = filtered_df[filtered_df['timestamp'].dt.date == pd.to_datetime(date_batch_a).date()].copy()
    batch_b = filtered_df[filtered_df['timestamp'].dt.date == pd.to_datetime(date_batch_b).date()].copy()
    return batch_a, batch_b

def compare_operations(batch_a, batch_b, operations, libraries):
    """Compare operations between the two batches."""
    comparison_rows = []
    for idx, row_b in batch_b.iterrows():
        script = row_b['script_name']
        fmt = row_b['dataset_format']

        # Find matching rows in Batch A
        batch_a_matches = batch_a[(batch_a['script_name'] == script) & (batch_a['dataset_format'] == fmt)]
        if batch_a_matches.empty:
            continue
        row_a = batch_a_matches.iloc[0]

        for op in operations:
            for lib in libraries:
                col_a = f"{op}_{lib}_seconds"
                col_b = f"{op}_{lib}prep_seconds"

                val_a = row_a[col_a]
                prep_a = row_a.get(col_b, np.nan)

                if pd.notna(val_a):
                    diff = val_b - val_a if (pd.notna(val_b) and pd.notna(val_a)) else np.nan
                    pct = (diff / val_a) * 100 if (pd.notna(diff) and val_a > 0) else np.nan

                    comparison_rows.append({
                        "Script": script,
                        "Format": fmt,
                        "Operation": op,
                        "Library": lib,
                        "Batch A (May 28)": val_a,
                        "Batch B (June 1)": row_b[col],
                        "Difference (s)": diff,
                        "Change (%)": pct,
                        "Prep Batch A (s)": prep_a,
                        "Prep Batch B (s)": row_b.get(col_b, np.nan)
                    })
    return pd.DataFrame(comparison_rows)

def print_summary(comp_df):
    """Print summaries formatted as markdown tables."""
    for (script, fmt), group in comp_df.groupby(["Script", "Format"]):
        print(f"\n======================================================================")
        print(f"SCRIPT: {script} | FORMAT: {fmt.upper()}")
        print(f"======================================================================")

        headers = ["Library", "Operation", "Batch A", "Batch B", "Diff (s)", "Change (%)"]
        print(f"| {' | '.join(headers) } |")
        print(f"|{'-' * 18}|{'-' * 26}|{'-' * 15}|{'-' * 19}|{'-' * 13}|{'-' * 16}|")

        for _, row in group.iterrows():
            lib = row["Library"]
            op = row["Operation"]
            val_a = f"{row['Batch A (May 28)']:.4f}s" if pd.notna(row['Batch A (May 28)']) else "N/A"
            val_b = f"{row['Batch B (June 1)']:.4f}s" if pd.notna(row['Batch B (June 1)']) else "N/A"
            diff = f"{row['Difference (s)']:.4f}s" if pd.notna(row['Difference (s)']) else "N/A"

            pct_val = row['Change (%)']
            if pd.isna(pct_val):
                pct = "N/A"
            else:
                sign = "+" if pct_val > 0 else ""
                pct = f"{sign}{pct_val:.2f}%"

            print(f"| {lib:<9} | {op:<14} | {val_a:>15} | {val_b:>15} | {diff:>12} | {pct:>13} |")

    prep_headers = ["Script", "Format", "Library", "Prep Batch A", "Prep Batch B", "Diff (s)"]
    print("\n\n======================================================================")
    print("PREPARATION TIMES COMPARISON (Batch A vs Batch B)")
    print("======================================================================")
    print(f"| {' | '.join(prep_headers) } |")
    print(f"|{'-' * 25}|{'-' * 8}|{'-' * 11}|{'-' * 14}|{'-' * 14}|{'-' * 10}|")

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

        print(f"| {script:<23} | {fmt:<6} | {lib:<9} | {p_a:>14} | {p_b:>14} | {diff:>12} |")
