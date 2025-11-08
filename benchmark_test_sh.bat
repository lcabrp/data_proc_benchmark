# Test on your current system (will show memory detection)
.\.venv\Scripts\python.exe .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv

# Force optimization to compare performance
.\.venv\Scripts\python.exe .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv -f

# Test custom threshold
.\.venv\Scripts\python.exe .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv -m 16