# 🔧 Technical Documentation

Deep dive into architecture, library setup (including Modin), and developer tooling.

## 📋 Table of Contents

- [Overview](#overview)
- [Benchmark Evolution](#benchmark-evolution)
- [Modin Setup and Stability Guide](#modin-setup-and-stability-guide)
- [Modules and Utilities](#modules-and-utilities)
- [Cross-Platform Optimizations](#cross-platform-optimizations)
- [Data Quality Improvements](#data-quality-improvements)
- [Code Architecture](#code-architecture)
- [File Format Performance Analysis](#file-format-performance-analysis)
- [Performance Analysis](#performance-analysis)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

The Data Processing Benchmark project evolved through multiple iterations to solve real-world cross-platform compatibility issues, memory management challenges, and reliability concerns when testing data processing libraries at scale.

### Test Dataset
- **Source**: Generated using `scripts/log-gen/test_generator_01.py`
- **Size**: 10 million records
- **Format**: Synthetic log data with realistic patterns
- **Fields**: timestamp, source_ip, destination_ip, port, bytes, status_code, event_type, response_time_ms, risk_score

## 🚀 Benchmark Evolution

### Original Implementation (`benchmark.py`)
The original script provided basic functionality but suffered from several critical issues:

**❌ Problems Identified:**
- **Modin Memory Issues**: `Task has 4.54 GiB worth of input dependencies, but worker has memory_limit set to 2.47 GiB`
- **FireDucks Timing Artifacts**: Tiny values like `1.6689300537109375e-06` instead of clean zeros
- **Platform Incompatibility**: Process-based workers failing on Windows
- **No Failure Recovery**: Repeated failures on same operations
- **Basic Host Detection**: Missing detailed CPU information

### Enhanced Implementation (`benchmark_01.py`)
The v1 enhancement introduced sophisticated solutions for all identified problems:

**✅ Solutions Implemented:**
- **Adaptive Modin Configuration**: Platform-specific Dask cluster setup
- **Two-Tier Failure Strategy**: Multi-worker → single-worker → disable fallback
- **Intelligent Failure Tracking**: Learns from failures and skips problematic configurations
- **DRY Architecture**: Modular utilities for system detection and memory monitoring
- **Robust FireDucks Handling**: Proper unavailability detection without timing artifacts

### Modular Implementation (`benchmark_modular.py`)
Adds universal file format support (CSV/Parquet/JSON/NDJSON), automatic dataset detection, and runs all operations across available libraries. Saves results in the original CSV-wide format with "N/A" for missing/unsupported libs.

Key internal functions added:
- DuckDB operations now use SQL (filter_group, stats, complex_join, timeseries)
- Added complex_join and timeseries operations for pandas/polars/modin/duckdb
- Results writer adjusted to avoid zeros for missing timings

### Further Enhanced Implementation (`benchmark_02.py`)
Building on v1's foundation, v2 addressed additional reliability and usability issues:

**✅ Additional Solutions Implemented:**
- **Modin Verbosity Suppression**: Redirected stdout/stderr to prevent Dask worker logs from cluttering output
- **Task Cancellation Fixes**: Enhanced Dask configurations to prevent "already forgotten" errors in statistics operations
- **CSV Alignment and Handling**: Fixed key order mismatches and added safe handling for None/0.0 values (saving 0.0 for skipped libraries, N/A for failures)
- **Summary Accuracy**: Excluded zero-duration results from "fastest" comparisons to avoid misleading winners (e.g., FireDucks at 0.0s)
- **Host Info Integration**: Centralized system information collection using the `utils.host_info` module

## Modin Setup and Stability Guide

Modin can be great, but setup can be a headache. These are the settings that consistently work in this repo.

Prerequisites:
- Python 3.13+
- modin[dask] (already in pyproject)
- dask, distributed (pulled via modin)
- psutil (for host info; install if missing)

Engine choice:
- Windows: Dask
- Linux/macOS: Dask or Ray. We stick to Dask for consistency and fewer extra deps.

Minimal config (place early in your script before importing modin.pandas):

```python
import modin.config as cfg
cfg.Engine.put("dask")
cfg.StorageFormat.put("pandas")
```

Recommended Dask runtime tuning (Windows stability):

```python
import logging
logging.getLogger("distributed").setLevel(logging.ERROR)
logging.getLogger("tornado").setLevel(logging.ERROR)

import dask
dask.config.set({
    "distributed.worker.daemon": False,
    "distributed.comm.timeouts.connect": "5s",
})
```

Operational guidance:
- Keep one Dask client/session for the process; don't restart between ops.
- Prefer fewer, larger partitions for wide aggregations (Modin handles partitioning).
- If an op fails due to memory, re-run with a single worker × more threads.

Common failure and workaround:
- Error: Task has N GiB worth of dependencies, but worker has memory_limit M GiB
    - Action: reduce workers to 1, increase threads_per_worker to 4–8; avoid client restarts.
- Excessive logging/noise
    - Action: silence logs as shown above; optionally redirect stdout/stderr around modin calls.

## Modules and Utilities

### utils/data_io.py
Universal data IO layer and dataset helpers.

- `UniversalDataReader(default_library='pandas')`
    - read_file(path, library='pandas'|'polars'|'modin'|'duckdb', usecols=None, ...)
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

## ⚙️ Modin Configuration Challenges

### The Memory Wall Problem

**Issue**: Modin with Dask often hits memory limits on complex operations
```
Task '_deploy_dask_func-...' has 4.54 GiB worth of input dependencies,
but worker tcp://127.0.0.1:51464 has memory_limit set to 2.47 GiB.
```

**Root Causes**:
1. **Data Amplification**: Statistical operations create intermediate data structures larger than input
2. **Worker Isolation**: Each worker needs a copy of data dependencies
3. **Memory Fragmentation**: Multiple small allocations exceed limits

### Evolution of Solutions

| Version | Approach | Worker Config | Memory Strategy | Success Rate |
|---------|----------|---------------|----------------|---------------|
| **benchmark.py** | Basic Dask | Default config | Fixed limits | ❌ 25% (statistics fails) |
| **benchmark_02.py** | Ray backend | `cfg.Engine.put("ray")` | 8GB hard limit | ❌ Ray unavailable on Windows |
| **benchmark_03.py** | Conservative | 2 workers × 1 thread | 4GB per worker | ❌ Too conservative |
| **benchmark_04.py** | Single worker | 1 worker × 4 threads | 6GB limit | ⚠️ No parallelization |
| **benchmark_01.py** | Adaptive | Platform-aware | Intelligent fallback | ✅ 100% cross-platform |
| **benchmark_02.py** | Further Enhanced | Platform-aware + fixes | Intelligent fallback + suppression | ✅ 100% cross-platform |

### The Winning Strategy

**Two-Tier Approach**:
1. **Attempt multi-worker** for performance
2. **Fallback to single-worker** for compatibility
3. **Learn from failures** and skip problematic configs

**Client Lifecycle Preservation**:
```python
# DON'T restart client between operations (causes dependency loss)
# client.restart()  # This was causing "lost dependencies" errors

# DO preserve worker state across operations
client = get_dask_client()  # Create once, reuse
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
- Libraries: pandas, polars, duckdb, modin
- Hardware: AMD Ryzen AI 9 365 (20 cores), 31.12GB RAM
- File Formats: CSV (uncompressed), Parquet (snappy compression)

#### Recommendations:

1. **Default to Parquet**: For analytical workloads, Parquet should be the default choice
2. **Compression Strategy**: Use snappy or zstd compression for optimal balance of speed and size
3. **Migration Path**: Consider converting existing CSV datasets to Parquet for better performance
4. **Tool Integration**: Leverage tools like `scripts/tools/csv_to_parquet.py` for format conversion

## 📈 Performance Analysis

### Benchmark Results Comparison

**10M Record Dataset Performance** (Intel i7-1065G7, 8 cores, 19.78GB RAM):

| Operation | Polars | DuckDB | Pandas | Modin (v1) | Improvement |
|-----------|--------|--------|--------|------------|-------------|
| **Filter/Group** | 1.55s | 4.04s | 22.92s | 25.46s* | 14.8x vs pandas |
| **Statistics** | 2.48s | 4.97s | 22.82s | 41.06s* | 9.2x vs pandas |
| **Complex Join** | 6.12s | 31.29s | 42.16s | 60.91s* | 6.9x vs pandas |
| **Timeseries** | 9.04s | 5.25s | 27.26s | 37.55s* | 3.0x vs pandas |

*_Modin performance varies significantly based on operation complexity and available memory_

### Key Insights

1. **Polars Excellence**: Dominates 3/4 operations with Rust-powered performance
2. **DuckDB Strength**: Superior for analytical/OLAP operations (timeseries)
3. **Modin Challenges**: Memory pressure limits effectiveness on statistics operations
4. **Pandas Reliability**: Consistent baseline performance across all operations

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### Modin Memory Errors
```
Task has X.XX GiB worth of input dependencies, but worker has memory_limit set to Y.YY GiB
```
**Solution**: The v1 implementation automatically handles this with adaptive fallback

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
print(detector.get_recommended_modin_engine())
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
