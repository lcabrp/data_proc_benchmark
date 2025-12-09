#!/bin/bash
# Benchmark test script with various optimization modes

# Test 1: Auto mode (default - use memory threshold detection)
echo "=== Test 1: Auto mode (default) ==="
python scripts/benchmark/benchmark.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv

# Test 2: Always optimize (force optimization regardless of RAM)
echo "=== Test 2: Always optimize ==="
python scripts/benchmark/benchmark.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv --optimize always

# Test 3: Never optimize (disable optimization even on low RAM)
echo "=== Test 3: Never optimize ==="
python scripts/benchmark/benchmark.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv --optimize never

# Test 4: Auto mode with custom threshold (32GB)
echo "=== Test 4: Auto mode with 32GB threshold ==="
python scripts/benchmark/benchmark.py -d data/raw/synthetic_logs_10M.csv -o data/benchmark_results.csv --optimize auto -m 32
