# Data Processing Performance Benchmark — Copilot Instructions

This repo provides a **comprehensive benchmarking suite** comparing popular Python data processing libraries (pandas, modin, polars, DuckDB, FireDucks) using real-world synthetic log datasets. Primary focus: execution time, memory usage (RSS delta tracking), and multi-machine performance comparison.

**Libraries:** pandas, modin, polars, DuckDB, FireDucks (Linux/macOS only)  
**Dataset Size:** 10M rows (synthetic server logs)  
**Tech Stack:** Python 3.13, psutil, pyarrow, platform-specific optimizations  
**Output:** CSV benchmarks, comparison analysis, host summaries

---

## Project Structure

```
data_proc_benchmark/
├── scripts/
│   └── benchmark/
│       ├── benchmark_modular.py       # ⭐ Main modular benchmark
│       └── compare_hosts.py           # Multi-machine comparison
├── utils/
│   ├── config.py                      # Project configuration
│   ├── data_io.py                     # Universal file reading
│   ├── host_info.py                   # System info collection
│   ├── platform_utils.py              # Platform/library detection
│   └── useful_functions.py            # Shared utilities
├── analysis/                          # Benchmark result analysis
├── data/                              # Test datasets (10M rows)
├── BENCHMARK_FINDINGS.md              # Detailed analysis & methodology
├── TECHNICAL.md                       # Technical deep dive
└── pyproject.toml                     # Dependencies
```

---

## Core Benchmarking Concepts

### Four Standard Operations

1. **Filter & Group:** Filter by status, group by source IP
2. **Statistics:** Aggregations by event type (mean, std, min, max, median)
3. **Complex Join:** Enrich rows with per-source aggregates and rankings
4. **Time Series:** Hourly rollups (with fallback if timestamps missing)

### Memory Tracking

**RSS Delta Approach:**
- Measure **Resident Set Size** before and after each operation
- Track peak memory during execution
- Report delta (peak - baseline) per operation

```python
import psutil

process = psutil.Process()
baseline_rss = process.memory_info().rss / (1024 ** 2)  # MB

# Run operation
result = perform_operation(df)

peak_rss = process.memory_info().rss / (1024 ** 2)
memory_delta = peak_rss - baseline_rss
```

### Platform Detection

**FireDucks availability:**
- Linux/macOS: Available
- Windows: Not supported (excluded from benchmarks)

```python
from utils.platform_utils import (
    FIREDUCKS_AVAILABLE,
    POLARS_AVAILABLE,
    DUCKDB_AVAILABLE
)

if FIREDUCKS_AVAILABLE:
    import fireducks.pandas as fpd
```

---

## Python/Analysis Conventions

### Modular Benchmark Pattern

**Standard usage:**

```bash
# Run benchmark with default dataset (auto-detected)
python scripts/benchmark/benchmark_modular.py

# Explicit dataset path
python scripts/benchmark/benchmark_modular.py -d data/dataset_10M.parquet

# Specify output file
python scripts/benchmark/benchmark_modular.py -o results/my_benchmark.csv

# Quick test (fewer iterations)
python scripts/benchmark/benchmark_modular.py --quick
```

### Universal Data Reader

Supports **CSV, Parquet, JSON** with automatic format detection:

```python
from utils.data_io import UniversalDataReader, DatasetFinder

# Initialize reader
reader = UniversalDataReader(default_library='pandas')

# Read any supported format
df = reader.read_file('data/dataset_10M.parquet')
df = reader.read_file('data/logs.csv')
df = reader.read_file('data/events.json')

# Auto-detect datasets in project
finder = DatasetFinder(
    search_dirs=['data/', 'analysis/'],
    file_patterns=['*.parquet', '*.csv', '*.json']
)
datasets = finder.find_datasets()
```

### Host Information Collection

```python
from utils.host_info import get_host_info

# Collect comprehensive system info
host_info = get_host_info()
# Returns: CPU model, core count, RAM, OS, Python version, library versions
```

### Data Type Optimization

```python
from utils.useful_functions import optimize_df_types

# Optimize DataFrame memory (downcast int64→int32/int16, etc.)
df = optimize_df_types(df)
```

---

## Benchmark Execution Patterns

### ModularBenchmark Class

```python
from scripts.benchmark.benchmark_modular import ModularBenchmark

# Initialize benchmark
benchmark = ModularBenchmark()

# Run all operations for a library
results = benchmark.run_library_benchmark(
    library_name='pandas',
    data=df,
    iterations=3
)

# Results include: execution time, memory delta, result shape
```

### Memory Measurement Pattern

```python
import psutil
import gc

def measure_operation(func, *args):
    """Measure execution time and memory delta"""
    gc.collect()  # Clean garbage before measurement
    process = psutil.Process()
    baseline_rss = process.memory_info().rss / (1024 ** 2)
    
    start = time.perf_counter()
    result = func(*args)
    elapsed = time.perf_counter() - start
    
    peak_rss = process.memory_info().rss / (1024 ** 2)
    memory_delta = peak_rss - baseline_rss
    
    return {
        'elapsed': elapsed,
        'memory_mb': memory_delta,
        'result': result
    }
```

### Outlier Removal (IQR Method)

**Critical for multi-machine comparisons:**

```python
def remove_outliers_iqr(rows, multiplier=1.5):
    """
    Remove rows where any library's mean is an extreme outlier.
    Uses IQR method: Q1 - 1.5×IQR to Q3 + 1.5×IQR
    """
    # Per library: Calculate Q1, Q3, IQR
    # Remove catastrophic failures while preserving valid variance
```

**Usage in compare_hosts.py:**

```bash
# Compare hosts with outlier removal
python scripts/benchmark/compare_hosts.py --remove-outliers

# Without outlier removal (raw data)
python scripts/benchmark/compare_hosts.py
```

---

## Multi-Machine Comparison

### Methodology Improvements

**Problem:** Initial comparisons were misleading due to:
1. Catastrophic outliers (52.8s vs 0.8s median)
2. Row count imbalance (24 vs 56 rows)
3. Mean as single metric (fails with outliers)

**Solution:** Use multiple metrics:
- **Median:** Robust to outliers
- **Best-case:** Hardware potential without contamination
- **Percentiles (P10/P25/P75/P90):** Distribution shape
- **IQR outlier removal:** Remove catastrophic failures

### Compare Hosts Usage

```bash
# Basic comparison (all CSV files in current directory)
python scripts/benchmark/compare_hosts.py

# With outlier removal (recommended)
python scripts/benchmark/compare_hosts.py --remove-outliers

# Specify CSV files explicitly
python scripts/benchmark/compare_hosts.py benchmark1.csv benchmark2.csv

# Custom multiplier for outlier detection
python scripts/benchmark/compare_hosts.py --remove-outliers --iqr-multiplier 2.0
```

**Output:**
```
=== HOST SUMMARY ===
Host: Legion7-16IRX9 (Intel i9-14900HX, 64GB)
Pandas mean: 1.13s | median: 0.90s | best: 0.52s
```

---

## Library-Specific Patterns

### pandas

```python
import pandas as pd

# Standard operations
filtered = df[df['status'] == 'error'].copy()
grouped = df.groupby('source_ip')['response_time'].mean()
```

### polars

```python
import polars as pl

# Lazy execution
lf = pl.scan_parquet('data/logs.parquet')
result = lf.filter(pl.col('status') == 'error').collect()

# Eager execution
df = pl.read_parquet('data/logs.parquet')
grouped = df.group_by('source_ip').agg(pl.col('response_time').mean())
```

### DuckDB

```python
import duckdb

# Direct SQL on DataFrame
result = duckdb.sql("""
    SELECT source_ip, AVG(response_time) as avg_time
    FROM df
    WHERE status = 'error'
    GROUP BY source_ip
""").df()

# From file
result = duckdb.sql("""
    SELECT * FROM 'data/logs.parquet'
    WHERE status = 'error'
""").df()
```

### FireDucks (Linux/macOS only)

```python
import fireducks.pandas as fpd

# Drop-in pandas replacement with optimizations
df = fpd.read_parquet('data/logs.parquet')
filtered = df[df['status'] == 'error']
grouped = df.groupby('source_ip')['response_time'].mean()
```

### modin[dask]

```python
import modin.pandas as mpd

# Parallel pandas operations
df = mpd.read_parquet('data/logs.parquet')
filtered = df[df['status'] == 'error']
grouped = df.groupby('source_ip')['response_time'].mean()
```

---

## Dataset Generation

### Creating 10M Row Test Datasets

```bash
# Windows
create_10M_datasets.bat

# Linux/macOS
./create_10M_datasets.sh
```

**Generates:**
- `data/dataset_10M.csv`
- `data/dataset_10M.parquet`
- `data/dataset_10M.json`

**Schema:**
- `timestamp`: DateTime
- `source_ip`: IP address
- `event_type`: Category (error, warning, info)
- `status`: HTTP status code
- `response_time`: Float (milliseconds)
- `user_id`: Integer
- `session_id`: UUID

---

## Benchmark Best Practices

### Memory Considerations

- **Always garbage collect** before measurements: `gc.collect()`
- **Measure RSS delta**, not absolute memory
- **Track peak memory** during operation
- **Close/delete intermediate results** in long benchmarks

### Timing Considerations

- **Use `time.perf_counter()`** for high-resolution timing
- **Multiple iterations:** 3-5 for stable results
- **Warm-up runs:** First run often slower (JIT, caching)
- **Disable other processes:** Minimize background activity

### Data Quality

- **Consistent datasets:** Same data across all libraries
- **Realistic operations:** Mirror real-world workflows
- **Multiple formats:** Test CSV, Parquet, JSON
- **Document outliers:** Explain anomalies in results

---

## Output & Analysis

### Benchmark CSV Format

```csv
Timestamp,Host,OS,Library,Operation,Mean,Median,Best,Worst,Iterations,MemoryMB
2026-04-26 10:30:00,MyLaptop,Windows,pandas,filter_group,1.234,1.189,1.145,1.402,5,245.3
```

### Key Metrics

- **Mean:** Average execution time (sensitive to outliers)
- **Median:** Middle value (robust to outliers)
- **Best:** Fastest run (hardware potential)
- **Worst:** Slowest run (includes outliers)
- **MemoryMB:** RSS delta in megabytes

---

## Common Workflows

### Running Standard Benchmark

1. **Generate dataset:** `create_10M_datasets.bat` (if needed)
2. **Run benchmark:** `python scripts/benchmark/benchmark_modular.py`
3. **Review results:** Check CSV output in current directory
4. **Analyze:** Use `compare_hosts.py` for multi-machine comparison

### Adding New Operation

1. Create new method in `ModularBenchmark` class
2. Add to `OPERATIONS` list
3. Test with `--quick` flag
4. Document in `BENCHMARK_FINDINGS.md`

### Comparing Machines

1. **Run benchmark on each machine:** Save CSV with descriptive name
2. **Copy CSVs to analysis directory**
3. **Run comparison:** `python scripts/benchmark/compare_hosts.py --remove-outliers`
4. **Document findings:** Update `BENCHMARK_FINDINGS.md`

---

## Platform-Specific Notes

### Windows

- FireDucks not available (excluded automatically)
- Use `benchmark_all_win.bat` for batch runs
- WMI library for detailed hardware info

### Linux/macOS

- All libraries available (including FireDucks)
- Use `benchmark_all_sh.sh` for batch runs
- Standard CPU info via `/proc/cpuinfo`

### WSL2

- Runs as Linux environment
- FireDucks available
- May show different performance vs native Windows

---

## References

- **Detailed Analysis:** `BENCHMARK_FINDINGS.md`
- **Technical Deep Dive:** `TECHNICAL.md`
- **Optimization Modes:** `OPTIMIZATION_MODE_USAGE.md`
- **Library Docs:** pandas, polars, DuckDB, FireDucks, modin
