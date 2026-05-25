import glob
import sys

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    changed = False
    if 'fetchdf()' in content:
        content = content.replace('fetchdf()', 'fetch_arrow_table()')
        changed = True
        
    if 'sys.stdout.encoding' not in content:
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if line.startswith('import sys'):
                lines.insert(i + 1, "if sys.stdout.encoding != 'utf-8':\n    sys.stdout.reconfigure(encoding='utf-8')")
                content = '\n'.join(lines)
                changed = True
                break
                
    if changed:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f'Processed {filepath}')

for fp in glob.glob('scripts/benchmark/benchmark*.py'):
    process_file(fp)
