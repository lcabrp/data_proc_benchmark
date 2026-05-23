REM Run all Scripts
REM csv
uv run python .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv -opt always
uv run python .\scripts\benchmark\benchmark_01.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv
uv run python .\scripts\benchmark\benchmark_02.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv
uv run python .\scripts\benchmark\benchmark_modular.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv

REM parquet
uv run python .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.parquet -o .\data\benchmark_results.csv -opt always
uv run python .\scripts\benchmark\benchmark_01.py -d .\data\raw\synthetic_logs_10M.parquet -o .\data\benchmark_results.csv
uv run python .\scripts\benchmark\benchmark_02.py -d .\data\raw\synthetic_logs_10M.parquet -o .\data\benchmark_results.csv
uv run python .\scripts\benchmark\benchmark_modular.py -d .\data\raw\synthetic_logs_10M.parquet -o .\data\benchmark_results.csv