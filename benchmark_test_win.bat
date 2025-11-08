@echo off
REM Benchmark test script with various optimization modes

REM Test 1: Auto mode (default - use memory threshold detection)
echo === Test 1: Auto mode (default) ===
.\.venv\Scripts\python.exe .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv

REM Test 2: Always optimize (force optimization regardless of RAM)
echo === Test 2: Always optimize ===
.\.venv\Scripts\python.exe .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv --optimize always

REM Test 3: Never optimize (disable optimization even on low RAM)
echo === Test 3: Never optimize ===
.\.venv\Scripts\python.exe .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv --optimize never

REM Test 4: Auto mode with custom threshold (32GB)
echo === Test 4: Auto mode with 32GB threshold ===
.\.venv\Scripts\python.exe .\scripts\benchmark\benchmark.py -d .\data\raw\synthetic_logs_10M.csv -o .\data\benchmark_results.csv --optimize auto -m 32
