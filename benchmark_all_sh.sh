#!/bin/bash
# filepath: c:\Users\lcabr\Documents\Projects\data_proc_benchmark\benchmark_all.sh
# Run all Scripts
# csv
python scripts/benchmark/benchmark.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv
python scripts/benchmark/benchmark_01.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv
python scripts/benchmark/benchmark_02.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv
python scripts/benchmark/benchmark_modular.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv

# parquet
python scripts/benchmark/benchmark.py -d data/raw/synthetic_logs_10M.parquet -o data/benchmark_results.csv
python scripts/benchmark/benchmark_01.py -d data/raw/synthetic_logs_10M.parquet -o data/benchmark_results.csv
python scripts/benchmark/benchmark_02.py -d data/raw/synthetic_logs_10M.parquet -o data/benchmark_results.csv
python scripts/benchmark/benchmark_modular.py -d data/raw/synthetic_logs_10M.parquet -o data/benchmark_results.csv