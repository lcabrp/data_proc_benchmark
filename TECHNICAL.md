# 🔧 Technical Documentation

Deep dive into architecture, library setup, and developer tooling.

## 📋 Table of Contents

- [Overview](#overview)
- [Benchmark Evolution](#benchmark-evolution)
- [Optimization Journey](#optimization-journey)
- [Modules and Utilities](#modules-and-utilities)
- [Cross-Platform Optimizations](#cross-platform-optimizations)
- [Data Quality Improvements](#data-quality-improvements)
- [Code Architecture](#code-architecture)
- [File Format Performance Analysis](#file-format-performance-analysis)
- [Performance Analysis](#performance-analysis)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

The Data Processing Benchmark project evolved through multiple iterations to solve real-world cross-platform compatibility issues, memory management challenges, and reliability concerns when testing data processing libraries at scale.

### Memory Optimization Strategy

Starting from version 0.1.5, the benchmark includes intelligent memory optimization with flexible user control:

**Optimization Techniques**:
- **Type optimization**: Converts DataFrame columns to optimal types (category, uint16/uint32, float32)
- **Memory reduction**: Achieves up to 94% reduction in memory footprint (5.5GB → 334MB)
- **Performance gain**: Provides 2-3x speedup for pandas/FireDucks operations on optimized data

**Conditional Application**:
- **Auto mode** (default): Applies optimization only when system memory < threshold
- **Always mode**: Forces optimization for consistent behavior across systems
- **Never mode**: Disables optimization for raw performance testing

**Example Optimizations** (10M row dataset):
```
Original DataFrame:
  timestamp         : object     → datetime64[ns]   (50% reduction)
  source_ip         : object     → category         (90% reduction)
  destination_ip    : object     → category         (90% reduction)
  port              : int64      → uint16           (75% reduction)
  bytes             : int64      → uint32           (50% reduction)
  response_time_ms  : int64      → uint16           (75% reduction)
  risk_score        : float64    → float32          (50% reduction)
  event_type        : object     → category         (90% reduction)
  
Overall: 5518.3MB → 333.8MB (94% reduction)
```

**System-Aware Defaults**:
- < 16GB RAM: Optimization enabled by default (critical for performance)
- ≥ 16GB RAM: Optimization disabled by default (raw performance priority)
- Customizable threshold via `--mem-threshold` flag

### Test Dataset
- **Source**: Generated using `scripts/log-gen/test_generator_01.py`
- **Size**: 10 million records
- **Format**: Synthetic log data with realistic patterns
- **Fields**: timestamp, source_ip, destination_ip, port, bytes, status_code, event_type, response_time_ms, risk_score

## 🚀 Benchmark Evolution

### Original Implementation (`benchmark.py`)
The original script provided basic functionality but suffered from several critical issues:

**❌ Problems Identified:**
- **FireDucks Timing Artifacts**: Tiny values like `1.6689300537109375e-06` instead of clean zeros
- **Platform Incompatibility**: Process-based workers failing on Windows
- **No Failure Recovery**: Repeated failures on same operations
- **Basic Host Detection**: Missing detailed CPU information

### Enhanced Implementation (`benchmark_01.py`)
The v1 enhancement introduced sophisticated solutions for all identified problems:

**✅ Solutions Implemented:**
- **Two-Tier Failure Strategy**: Multi-worker → single-worker → disable fallback
- **Intelligent Failure Tracking**: Learns from failures and skips problematic configurations
- **DRY Architecture**: Modular utilities for system detection and memory monitoring
- **Robust FireDucks Handling**: Proper unavailability detection without timing artifacts

### Modular Implementation (`benchmark_modular.py`)
Adds universal file format support (CSV/Parquet/JSON/NDJSON), automatic dataset detection, and runs all operations across available libraries. Saves results in the original CSV-wide format with "N/A" for missing/unsupported libs.

Key internal functions added:
- DuckDB operations now use SQL (filter_group, stats, complex_join, timeseries)
- Added complex_join and timeseries operations for pandas/polars/duckdb
- Results writer adjusted to avoid zeros for missing timings

### Memory Instrumentation & Join Optimization (Post 0.1.2 → 0.1.3)

`benchmark_modular.py` gained two major enhancements for resource transparency and lower peak memory usage:

1. **Per‑Operation Memory Deltas**
    - Tracks RSS before & after each operation (psutil) and records delta MB.
    - Enables profiling which libraries/operations cause the largest working set expansions.
    - Helps distinguish inherent algorithm cost vs data format impact.

2. **Complex Join Refactor**
    - Pandas: replaced materializing an aggregated summary + merge with in‑place `groupby().transform(...)` assignments (sum/mean metrics + rank). Eliminates an intermediate wide join DataFrame.
    - Polars: replaced join-based enrichment with a single lazy pipeline applying window functions (`sum().over()`, `mean().over()`, `rank().over()`) followed by top‑N filter before `.collect()`. Reduces materialization and leverages predicate pruning.
    - DuckDB path unchanged (SQL window plan already efficient).
    - Polars timeseries now optimizes UTF-8 timestamp columns by extracting the hour with `str.slice(11, 2).cast(pl.UInt8)` instead of parsing full timestamps for every row, while still falling back to `.dt.hour()` for native datetime columns.

3. **Result Trimming (Optional Behavior)**
    - For complex join, only top‑ranked rows (bytes_rank ≤ 10) materialized, limiting large intermediate outputs retained in Python space.

4. **Garbage Collection & Early Deletion**
    - Large intermediates explicitly `del`'d prior to `gc.collect()` to hint at earlier release; helpful on CPython with large object graphs.

Limitations / Next Steps:
    - Column-subset selective reading planned (partial patch attempts) but not yet uniformly applied to all operations.
    - `--repeat` flag still parsed but not leveraged for averaging in modular script.
    - Memory tracking currently coarse (RSS delta); deeper instrumentation (peak tracking via tracemalloc) deferred to a later version.

### Further Enhanced Implementation (`benchmark_02.py`)
Building on v1's foundation, v2 addressed additional reliability and usability issues:

**✅ Additional Solutions Implemented:**
- **CSV Alignment and Handling**: Fixed key order mismatches and added safe handling for None/0.0 values (saving 0.0 for skipped libraries, N/A for failures)
- **Summary Accuracy**: Excluded zero-duration results from "fastest" comparisons to avoid misleading winners (e.g., FireDucks at 0.0s)
- **Host Info Integration**: Centralized system information collection using the `utils.host_info` module

### Unified CLI Interface

All maintained scripts (`benchmark.py`, `benchmark_01.py`, `benchmark_02.py`, `benchmark_modular.py`) now accept:

```
    -d / --dataset          Dataset path (auto-detect fallback where supported)
    -o / --output           Results CSV path (default data/benchmark_results.csv)
    --optimize / -opt       Memory optimization mode: auto (default), always, or never
    --mem-threshold / -m    Memory threshold in GB for auto mode (default: 16)
    --repeat N              Repeat count (default 1; ignored if a script doesn't implement repetition)
```

#### Memory Optimization Modes

The `--optimize` flag provides tri-state control over memory optimization:

**Auto Mode (default)**:
- Checks system memory against threshold (default: 16GB)
- Applies optimization if memory < threshold
- Customizable via `--mem-threshold` / `-m`
- Example: `--optimize auto -m 32` (optimize if system has < 32GB)

**Always Mode**:
- Forces optimization regardless of system memory
- Useful for testing optimized performance on high-RAM systems
- Ensures consistent behavior across machines
- Example: `--optimize always`

**Never Mode**:
- Disables optimization even on low-memory systems
- Useful for benchmarking raw (non-optimized) performance
- Allows comparison against optimized runs
- Example: `--optimize never`

**Performance Impact**:
- Optimization provides **2-3x speedup** for pandas/FireDucks operations
- Reduces memory usage by **94%** (5.5GB → 334MB for 10M rows)
- Critical for systems with < 16GB RAM
- Minimal overhead on high-memory systems when disabled

**Result Tracking**:
Results are tagged in CSV with optimization context:
- `benchmark.py_opt_always` - Forced optimization
- `benchmark.py_opt_never` - Optimization disabled
- `benchmark.py_opt_auto_mem15GB` - Auto mode, optimized (15GB system)
- `benchmark.py_no_opt_auto_mem64GB` - Auto mode, not optimized (64GB system)

Removed legacy flags `--csv` and `--results` (previously only in `benchmark_01.py`). Update any local invocation scripts accordingly.

## ⏱ Optimization Journey

Over the course of development, multiple deep architectural optimizations were implemented to tackle specific library bottlenecks and memory constraints when handling the 10M record dataset:

### 1. Unified Memory Optimization
- **Problem**: Pandas and FireDucks were consuming extreme amounts of memory (often crashing) when loading the 10M record dataset as raw `object` and `float64` types.
- **Solution**: We implemented `BENCHMARK_OPTIMIZATION_TYPES` to convert column types (e.g., `float64` → `float32`, `object` → `category`) natively. This reduced the DataFrame memory footprint by up to 94% (5.5GB → 334MB) in a single step.

### 2. Read-Time DType Injection (PyArrow Engine)
- **Problem**: While the memory optimization was effective, *applying* the optimization *after* loading the DataFrame was extremely slow (~12+ seconds).
- **Solution**: We shifted the conversion to *read time* by passing `engine="pyarrow"` and `dtype_backend="pyarrow"` directly into `pd.read_csv()`. This prevented the data from ever being instantiated in memory as slow `object` types, drastically reducing preparation time.

### 3. File-Based Caching
- **Problem**: Consecutive benchmark script runs (e.g., running `benchmark_01.py` then `benchmark_02.py`) were paying the heavy price of loading and parsing the raw CSV every single time.
- **Solution**: We introduced dataset caching (saving the optimized representation to `data/cache/optimized/...`). If an optimized version of the dataset already exists, subsequent runs load it instantaneously from Parquet, bypassing massive amounts of redundant prep work.

### 4. DuckDB Memory Management (OOM Prevention)
- **Problem**: DuckDB was previously throwing Out-Of-Memory (OOM) errors during heavy aggregation because it was fighting for memory space with the cached Pandas DataFrames holding the 10M records.
- **Solution**: We isolated the operations, ensuring Pandas dataframes are explicitly garbage collected (`del df; gc.collect()`) before DuckDB runs. We also ensured DuckDB processes had clear memory ceilings.

### 5. DuckDB Complex Join Optimization (Arrow Materialization)
- **Problem**: DuckDB's `complex_join` was taking ~12-13 seconds, making it the slowest operation by far. However, DuckDB was actually calculating the results instantly but stalling when converting the output back to a Pandas DataFrame using `.fetchdf()`. Additionally, DuckDB was double-scanning the 10M row disk file for self-joins.
- **Solution**: We introduced `--duckdb-mode cached` to load the file into a temporary memory table, avoiding disk double-scans. Most importantly, we replaced `.fetchdf()` with `.fetch_arrow_table()`, leveraging zero-copy PyArrow materialization. This instantly dropped the execution time from ~13s to ~4s!

## 🔗 CleanFlow Dependency

`data-proc-benchmark` delegates its core data-loading and optimization pipeline to [**cleanflow**](https://github.com/lcabrp/cleanflow), a sibling library maintained in a separate repository. This follows the **DRY** (Don't Repeat Yourself) principle: instead of duplicating file-reading, dtype-casting, and caching logic across four benchmark scripts, everything lives in one well-tested library that is *dogfooded* directly by these benchmarks.

### Sibling Repository Layout

Both repos must sit **side by side** in the same parent directory:
```
Projects/
├── cleanflow/            ← sibling library (source of truth for I/O & optimization)
└── data-proc-benchmark/  ← this repo
```

### Installation (one-time, per environment)

**Using uv (recommended):**
```bash
# From inside the data-proc-benchmark directory, after running `uv sync`:
uv pip install -e ../cleanflow
```

**Using pip (inside activated venv):**
```bash
pip install -e ../cleanflow
```

The `-e` / editable flag means Python resolves imports directly from the `cleanflow/` source folder on disk. A `git pull` in that folder is instantly reflected — no reinstall required.

**Verifying the install:**
```bash
uv run python -c "import cleanflow; print(cleanflow.__version__)"
# or inside the activated venv:
python -c "import cleanflow; print(cleanflow.__version__)"
```

### What CleanFlow Provides

| CleanFlow module | Responsibility in this project |
| :--- | :--- |
| `cleanflow.io.load_dataset()` | Unified entry point — detects format, applies dtype hints, manages the Parquet cache |
| `cleanflow.io.load_csv()` | Reads CSV with optional PyArrow engine (`engine='pyarrow'`, `dtype_backend='pyarrow'`) and read-time dtype injection |
| `cleanflow.io.load_parquet()` | Reads raw Parquet via `pd.read_parquet()`; after loading, `load_dataset` applies `type_map` casts before writing to cache |
| `cleanflow.apply_optimization()` | Post-load dtype downcasting (float64→float32, int64→uint32, object→category) using unsigned-aware `smallest_int_dtype` |
| `cleanflow.optimization.backends.duckdb_backend` | Zero-copy `fetch_arrow_table()` output from DuckDB queries |

### Optimized Parquet Cache Pipeline

The cache key is a SHA-1 hash of: `{file path, file size, mtime_ns, type_map}`. This means:
- Different `type_map` configurations produce different cache files.
- If the source file changes on disk, the old cache is ignored and a new one is written.
- Cache files are stored in `data/cache/optimized/` by default (configurable via `--optimized-cache-dir`).

**Flow for a Parquet source:**
```
First run:
  raw .parquet → pd.read_parquet() → apply type_map casts → write optimized .parquet to cache
                                                               ↑ ~8s one-time cost

Subsequent runs:
  cache hit → pd.read_parquet(optimized_cache) ← ~0.22s
```

**Flow for a CSV source:**
```
First run:
  .csv → pd.read_csv(engine=pyarrow, dtype hints) → write optimized .parquet to cache
                                                      ↑ ~4-5s one-time cost

Subsequent runs:
  cache hit → pd.read_parquet(optimized_cache) ← ~0.23s
```

---

## Modules and Utilities


### utils/data_io.py
Universal data IO layer and dataset helpers.

- `UniversalDataReader(default_library='pandas')`
    - read_file(path, library='pandas'|'polars'|'duckdb', usecols=None, ...)
    - Auto-detects file format: csv (incl .gz/.zip/.zst), parquet, json, ndjson
    - Returns a DataFrame in the chosen library

- `DatasetFinder(search_dirs: list[Path], file_patterns: list[str])`
    - `find_dataset(project_root) -> Path | None`
    - Scans typical locations under `data/raw` and `data/processed`

- `get_dataset_size(path: Path) -> int`
    - Efficient row counting across supported formats

### utils/host_info.py
Collects host/system metadata for CSV results.

Requires `psutil`; optionally uses `py-cpuinfo`.

Returns dictionary with:
- CPU/memory counts and frequencies
- Platform, Python version/implementation
- CPU brand/arch (with graceful fallbacks)

### utils/benchmark_schema.py
Single source of truth for constants used by all benchmark scripts.

- `BENCHMARK_OPTIMIZATION_TYPES`
    - Maps target dtypes (`category`, `uint16`, `uint32`, `float32`, `datetime64[ns]`) to the column lists used by `load_pandas_like_for_benchmark` and `optimize_df_types`.
- `OPERATION_ORDER`
    - Canonical order of benchmark operations: `filter_group`, `statistics`, `complex_join`, `timeseries`.
- `LIBRARY_ORDER`
    - Canonical order of libraries: `pandas`, `polars`, `duckdb`, `fireducks`.

Centralizing these values prevents the four benchmark scripts from drifting out of sync when columns are added or optimization rules change.

### utils/benchmark_operations.py
Shared benchmark operation implementations using the **strategy pattern**.

Each operation is a class that implements the same logical work across all supported libraries:

- `FilterGroupOperation`
    - Filter `bytes > 1000`, group by `event_type`, count rows.
- `StatisticsOperation`
    - Group by `event_type`, compute mean/min/max for `bytes`, `response_time_ms`, and `risk_score`.
- `ComplexJoinOperation`
    - Sum `bytes` by `source_ip`, join back, rank by `total_bytes` within each `event_type`, keep top 10.
    - Configurable `rank_col` (e.g., `bytes_rank` in `benchmark.py`, `total_rank` elsewhere) and `sort_by_rank`.
- `TimeseriesOperation`
    - Extract hour from `timestamp`, group by `(hour, event_type)`, count.
    - Configurable `hour_name` and `reset_index` to preserve each script's historical output shape.

Each class exposes:
- `run_pandas(df)` / `run_fireducks(df)` - FireDucks reuses the pandas path by default.
- `run_polars(df)`
- `run_duckdb(expr, con)` - Executes the equivalent SQL via a DuckDB connection.

Benchmark scripts keep thin adapter functions/methods that load data (or use a cached DataFrame) and call the appropriate `run_*` method. This preserves script-specific defaults while eliminating the duplication of 16 functions (4 operations × 4 libraries) that previously existed across the four scripts.

### scripts/tools/csv_to_parquet.py
Convert CSV/NDJSON to Parquet with chunking and compression.

Usage (PowerShell on Windows):
```powershell
# CSV -> Parquet (snappy)
.\.venv\Scripts\python scripts/tools/csv_to_parquet.py --input data\raw\logs.csv --out data\raw\logs.parquet

# Compressed CSV -> Parquet (zstd)
.\.venv\Scripts\python scripts/tools/csv_to_parquet.py --input data\raw\logs.csv.gz --out data\raw\logs.parquet --compression zstd

# NDJSON/JSONL -> Parquet
.\.venv\Scripts\python scripts/tools/csv_to_parquet.py --input data\raw\logs.ndjson --out data\raw\logs.parquet --format ndjson
```

Notes:
- Requires `pyarrow`.
- Supports partitioned output via `--partition col`.
- Processes large files in chunks for reasonable memory usage.

## 🌐 Cross-Platform Optimizations

### Windows-Specific Optimizations

**Thread-Based Workers**: Windows has issues with process-based parallelization in Dask
```python
LocalCluster(processes=False)  # Use threads, not processes
```

**Conservative Memory Limits**: Windows memory management is more restrictive
```python
memory_limit='6GB'  # Lower than Linux equivalent
```

**Single-Worker Fallback**: When multi-worker fails, fallback to single-worker with more threads
```python
threads_per_worker=4  # Compensate for fewer workers with more threads
```

### Linux/macOS Optimizations

**Process-Based Workers**: Better performance with process isolation
```python
LocalCluster(n_workers=4, threads_per_worker=2)
```

**FireDucks Support**: Full library availability detection
```python
if platform.system() in ['Linux', 'Darwin']:
    try:
        import fireducks.pandas as fpd
        FIREDUCKS_AVAILABLE = True
    except ImportError:
        FIREDUCKS_AVAILABLE = False
```

## 📊 Data Quality Improvements

### CPU Information Enhancement

**Problem**: Missing CPU brand information in CSV output
```csv
# Before: Generic processor info
processor,"Intel64 Family 6 Model 126 Stepping 5, GenuineIntel"
cpu_brand,"Unknown (cpuinfo not available)"
```

**Solution**: Integrated `cpuinfo` library with proper fallback handling
```python
try:
    import cpuinfo
    cpu_info = cpuinfo.get_cpu_info()
    info['cpu_brand'] = cpu_info.get('brand_raw', 'Unknown')
    info['cpu_arch'] = cpu_info.get('arch', 'Unknown')
except ImportError:
    info['cpu_brand'] = 'Unknown (cpuinfo not available)'
    info['cpu_arch'] = 'Unknown'
```

**Result**: Detailed CPU information

### Dataset Format Tracking

**Problem**: Benchmark comparisons across file formats (CSV vs Parquet vs NDJSON) required manually remembering which dataset form was used.

**Solution**: Added two new columns to the results CSV schema:
 - `dataset_name`: The full filename of the source dataset (e.g. `synthetic_logs_10M.parquet`)
 - `dataset_format`: Normalized logical format (csv, parquet, json, ndjson) with compression extensions removed (e.g. `.csv.gz` -> `csv`)

**Normalization Rules**:
 - Compression suffixes (`.gz`, `.zip`, `.zst`, `.bz2`) are stripped
 - `.jsonl` and `.ndjson` mapped to `ndjson`
 - Unknown/parse failure -> `unknown`

**Impact**: Enables longitudinal performance tracking segmented by storage format without needing external joins or filename parsing.
```csv
cpu_brand,"Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz"
cpu_arch,"X86_64"
```

### CSV Column Alignment Fix

**Problem**: Column misalignment due to incorrect dictionary insertion order
```csv
# Wrong order causing data shifts
4,3.13.5,CPython,1498.0,1298.0,19.78,9.45,...
```

**Solution**: Fixed dictionary construction order to match CSV header
```python
info = {
    'cpu_count_physical': psutil.cpu_count(logical=False),
    'cpu_freq_max': cpu_freq.max,
    'cpu_freq_current': cpu_freq.current,
    'memory_total_gb': round(mem.total / (1024**3), 2),
    'memory_available_gb': round(mem.available / (1024**3), 2),
    'python_version': platform.python_version(),
    'python_implementation': platform.python_implementation(),
    'cpu_brand': cpu_info.get('brand_raw', 'Unknown'),
    'cpu_arch': cpu_info.get('arch', 'Unknown')
}
```

## 🏗️ Code Architecture

### Modular Design Principles

**Separation of Concerns**:
- `host_info.py`: System information collection
- `memory_utils.py`: Memory monitoring and logging
- `platform_utils.py`: Platform detection and library availability
- `benchmark_schema.py`: Shared benchmark constants (optimization types, operation/library ordering)
- `benchmark_operations.py`: Shared benchmark operation implementations (strategy pattern)
- `benchmark_01.py`: Core benchmarking logic (unchanged per requirements)

**DRY Implementation**:
- **Before**: ~70 lines of duplicated host detection code
- **After**: Centralized utilities with clean imports

```python
# Clean imports in benchmark_01.py
from utils import (
    get_host_info, get_memory_usage_mb, log_memory_usage,
    PlatformDetector, SystemInfo
)
```

### Import Resolution

**Problem**: `ModuleNotFoundError: No module named 'utils'` when running from different directories

**Solution**: Dynamic path resolution
```python
# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
```

## 📊 File Format Performance Analysis

### Parquet vs CSV Performance Comparison

Recent benchmarking reveals significant performance advantages when using columnar formats like Parquet over traditional row-based formats like CSV. The analysis demonstrates the value of format selection in data processing workloads.

#### Key Findings (AMD Ryzen AI 9 365, 20 cores, 31.12GB RAM, 10M records):

| Library | Format | Filter/Group | Statistics | Complex Join | Timeseries | Average Improvement |
|---------|--------|-------------|------------|--------------|-----------|-------------------|
| **Pandas** | CSV | 15.69s | N/A | 34.07s | 20.19s | **2.8x faster with Parquet** |
| **Pandas** | Parquet | 5.57s | N/A | N/A | 3.31s | - |
| **DuckDB** | CSV | 0.62s | N/A | 7.00s | 0.21s | **8.9x faster with Parquet** |
| **DuckDB** | Parquet | 0.07s | N/A | 0.19s | 0.02s | - |

#### Performance Insights:

1. **Columnar Format Advantage**: Parquet consistently outperforms CSV across all operations and libraries
2. **DuckDB Optimization**: Shows the most dramatic improvement (8.9x) due to native columnar processing
3. **Pandas Benefits**: Even traditional libraries see significant gains (2.8x) with columnar formats
4. **Memory Efficiency**: Parquet's compression and columnar storage reduce I/O overhead
5. **Query Optimization**: Columnar formats enable better predicate pushdown and data skipping

#### Technical Explanation:

**Why Parquet Performs Better:**
- **Columnar Storage**: Data is stored by column rather than by row, enabling better compression
- **Compression Efficiency**: Similar data types in columns compress much better than mixed row data
- **I/O Reduction**: Only relevant columns need to be read for analytical queries
- **Predicate Pushdown**: Query conditions can be applied during data reading
- **Metadata Rich**: Parquet includes statistics and indexes for query optimization

**Benchmark Configuration:**
- Dataset: 10 million synthetic log records
- Operations: Filter/Group, Statistics, Complex Join, Timeseries
- Libraries: pandas, polars, duckdb
- Hardware: AMD Ryzen AI 9 365 (20 cores), 31.12GB RAM
- File Formats: CSV (uncompressed), Parquet (snappy compression)

#### Recommendations:

1. **Default to Parquet**: For analytical workloads, Parquet should be the default choice
2. **Compression Strategy**: Use snappy or zstd compression for optimal balance of speed and size
3. **Migration Path**: Consider converting existing CSV datasets to Parquet for better performance
4. **Tool Integration**: Leverage tools like `scripts/tools/csv_to_parquet.py` for format conversion

## 📈 Performance Analysis

### DuckDB Fetch Mode & Cache Optimization
When benchmarking DuckDB in memory-bound scenarios (like the 10M record dataset), two critical optimizations were identified and implemented:
1. **Double Scan Elimination (`--duckdb-mode cached`)**: By default, DuckDB reads directly from the source file (e.g. `read_parquet('...')`). For complex queries involving CTEs or self-joins (like the `complex_join` operation), DuckDB would scan the disk twice, severely degrading performance. Using `--duckdb-mode cached` forces DuckDB to load the file into memory as a `benchmark_source` temporary table, entirely bypassing the double-scan penalty.
2. **Arrow Materialization (`.fetch_arrow_table()`)**: DuckDB executes queries rapidly, but converting large result sets (e.g., 10M rows from a window function rank tie) back to a `pandas.DataFrame` using `.fetchdf()` introduced up to 7+ seconds of pure overhead. The scripts were updated to use `.fetch_arrow_table()` natively, enabling DuckDB to emit results in zero-copy columnar format and align its reported execution time more closely with pure query performance.

### Benchmark Results Comparison

**10M Record Dataset Performance** (Intel i7-1065G7, 8 cores, 19.78GB RAM):

| Operation | Polars | DuckDB | Pandas | Improvement |
|-----------|--------|--------|--------|-------------|
| **Filter/Group** | 1.55s | 4.04s | 22.92s | 14.8x vs pandas |
| **Statistics** | 2.48s | 4.97s | 22.82s | 9.2x vs pandas |
| **Complex Join** | 6.12s | 31.29s | 42.16s | 6.9x vs pandas |
| **Timeseries** | 9.04s | 5.25s | 27.26s | 3.0x vs pandas |

### Memory Optimization Impact Analysis

**10M Record Dataset** (AMD Ryzen 9 8945HS, 16 cores, 15.3GB RAM, WSL2):

#### Optimized vs Non-Optimized Performance Comparison:

| Library | Operation | Optimized | Non-Optimized | Slowdown | Memory |
|---------|-----------|-----------|---------------|----------|--------|
| **Pandas** | Filter/Group | 0.51s | 1.41s | **2.8x slower** | 16.5x larger |
| **Pandas** | Statistics | 0.31s | 0.95s | **3.1x slower** | 16.5x larger |
| **Pandas** | Complex Join | 3.79s | 8.99s | **2.4x slower** | 16.5x larger |
| **Pandas** | Timeseries | 0.65s | 3.28s | **5.0x slower** | 16.5x larger |
| **FireDucks** | Filter/Group | 0.39s | 1.39s | **3.6x slower** | 16.5x larger |
| **FireDucks** | Statistics | 0.37s | 1.04s | **2.8x slower** | 16.5x larger |
| **FireDucks** | Complex Join | 3.45s | 8.39s | **2.4x slower** | 16.5x larger |
| **FireDucks** | Timeseries | 0.71s | 2.91s | **4.1x slower** | 16.5x larger |

**Key Findings**:
1. **Critical for Low-RAM Systems**: Without optimization, pandas/FireDucks are 2-5x slower
2. **Massive Memory Savings**: 94% reduction (5518MB → 334MB) enables benchmarking on 16GB systems
3. **Polars/DuckDB Unaffected**: These libraries maintain consistent performance regardless of pandas optimization
4. **Timeseries Most Impacted**: 4-5x slowdown without optimization due to datetime operations
5. **Optimization Mode Control**: `--optimize` flag enables flexible testing of both scenarios

### Key Insights

1. **Polars Excellence**: Dominates 3/4 operations with Rust-powered performance
2. **DuckDB Strength**: Superior for analytical/OLAP operations (timeseries)
3. **Pandas Reliability**: Consistent baseline performance across all operations
4. **Memory Optimization Critical**: 2-5x performance impact on systems with < 16GB RAM

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### DuckDB Complex Join Failures in WSL2

DuckDB's complex join benchmark can need more memory than WSL2 exposes by default. Microsoft documents WSL2's default VM memory allocation as 50% of total Windows memory, with optional disk-backed swap configured through `%UserProfile%\.wslconfig`: https://learn.microsoft.com/en-us/windows/wsl/wsl-config

If DuckDB fails in WSL2 during `complex_join`, add or update `C:\Users\<your_windows_user>\.wslconfig`:

```ini
[wsl2]

# Extra disk-backed memory for high-memory DuckDB operations.
swap=32GB

# Optional: choose a fixed swap VHDX location.
# Make sure the folder exists and the drive has enough free space.
swapFile=D:/WSL/wsl-swap.vhdx

# Optional: on WSL builds where memory=0 removes the VM cap, this lets
# WSL grow beyond the default half-RAM allocation. If it is ignored on
# your system, use an explicit cap such as memory=48GB instead.
memory=0

localhostForwarding=true
```

After changing `.wslconfig`, restart WSL from PowerShell:

```powershell
wsl --shutdown
```

Then reopen the distro and confirm the new limits from inside WSL:

```bash
free -h
```

The benchmark scripts create and use a DuckDB temp/spill directory automatically:

- Windows: under `%TEMP%\data-proc-benchmark-duckdb`
- WSL/Linux: under `/tmp/data-proc-benchmark-duckdb`

You do not need to export environment variables on every PC. Use these only when you want to override the defaults for a specific test:

```sql
SET preserve_insertion_order = false;
SET threads = 4;
SET temp_directory = '/tmp/data-proc-benchmark-duckdb';
SET max_temp_directory_size = '32GB';
```

Do not use `SET memory_limit = '0'`. In DuckDB 1.3.2 this is a parser error, and DuckDB's OOM guide recommends reducing `memory_limit` below the default 80% of RAM when the OS kills the process because some allocations are outside the buffer manager. If needed, set an explicit limit such as `SET memory_limit = '12GB'` or about 50-60% of the memory shown by `free -h`.

#### FireDucks Import Issues on Windows
```
ModuleNotFoundError: No module named 'fireducks'
```
**Solution**: This is expected - FireDucks is Linux/macOS only. v1 handles gracefully.

#### CSV Column Misalignment
```
Data appears in wrong columns in CSV output
```
**Solution**: Fixed in v1 with proper dictionary ordering in `get_host_info()`

#### Import Path Issues
```
ModuleNotFoundError: No module named 'utils'
```
**Solution**: v1 includes automatic project root detection and path insertion

### Debugging Tools

**Memory Monitoring**:
```python
from utils import log_memory_usage
log_memory_usage("Operation start")
# ... run operation ...
log_memory_usage("Operation end")
```

**Platform Detection**:
```python
from utils import PlatformDetector
detector = PlatformDetector()
print(detector.get_platform_flags())
```

## 🔮 Future Enhancements

### Planned Improvements
1. **GPU Acceleration**: RAPIDS cuDF integration for CUDA-enabled systems
2. **Distributed Testing**: Multi-node Dask cluster support
3. **Memory Profiling**: Detailed memory usage tracking per operation
4. **Custom Metrics**: Operation-specific performance indicators
5. **Automated Reporting**: HTML dashboard generation from CSV results

### Extension Points
- **New Libraries**: Add support for Vaex, Koalas, or other emerging libraries
- **Custom Operations**: Domain-specific benchmarks (e.g., geospatial, NLP)
- **Cloud Integration**: S3/Azure Blob storage for large dataset testing
- **Containerization**: Docker images for consistent cross-platform testing

---

**📝 Note**: This technical documentation reflects the state as of September 2025. The benchmark continues to evolve based on community feedback and new library developments.
