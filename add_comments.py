import glob

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Don't add twice
    if 'to eliminate severe pandas conversion overhead' in content and filepath != 'scripts/benchmark\\benchmark.py':
        return

    # Replace lines ending with .fetch_arrow_table()
    lines = content.split('\n')
    new_lines = []
    changed = False
    
    for line in lines:
        if '.fetch_arrow_table()' in line and 'Optimization (2026-05-24)' not in line:
            indent = line[:len(line) - len(line.lstrip())]
            new_lines.append(f"{indent}# Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()")
            new_lines.append(f"{indent}# to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.")
            new_lines.append(line)
            changed = True
        elif '--duckdb-mode cached' in line or 'duckdb_mode="cached"' in line or 'duckdb-mode: cached' in line:
            # Maybe comment where duckdb-mode cached is used or parsed
            new_lines.append(line)
        else:
            new_lines.append(line)
            
    if changed:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(new_lines))
        print(f"Updated {filepath}")

for fp in glob.glob('scripts/benchmark/benchmark*.py'):
    process_file(fp)

