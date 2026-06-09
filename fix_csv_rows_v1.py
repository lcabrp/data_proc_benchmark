import csv
from pathlib import Path

csv_path = Path("data/benchmark_results.csv")

with open(csv_path, "r", encoding="utf-8", newline="") as f:
    reader = csv.reader(f)
    rows = list(reader)

if not rows:
    print("CSV is empty")
    exit()

header = rows[0]
target_len = len(header)
print(f"Header length: {target_len}")

fixed_rows = [header]
fixed_count = 0

for i, row in enumerate(rows[1:], start=2):
    current_len = len(row)
    if current_len < target_len:
        # Pad with empty strings
        row.extend([""] * (target_len - current_len))
        fixed_count += 1
    elif current_len > target_len:
        # Truncate
        row = row[:target_len]
        fixed_count += 1
    fixed_rows.append(row)

if fixed_count > 0:
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(fixed_rows)
    print(f"Fixed {fixed_count} rows in the CSV.")
else:
    print("All rows are already consistent.")
