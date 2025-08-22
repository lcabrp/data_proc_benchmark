# Comprehensive Data Processing Benchmark

This enhanced benchmark tests multiple data processing libraries across different types of operations using a 10-million record dataset.

## Features

### 🚀 **Libraries Tested**
- **Pandas**: Traditional data processing library
- **Modin**: Distributed pandas with Dask backend
- **Polars**: Fast DataFrame library written in Rust
- **DuckDB**: In-process SQL OLAP database
- **FireDucks**: High-performance pandas-compatible library (Linux/macOS only)

### 📊 **Benchmark Operations**

1. **Filter & Group** (Original operation)
   - Filter records where `status_code == 200`
   - Group by `source_ip` and sum `bytes`

2. **Statistical Analysis** (New)
   - Group by `event_type`
   - Calculate mean, std, min, max for `bytes`
   - Calculate mean, median for `response_time_ms` and `risk_score`

3. **Complex Join & Window Functions** (New)
   - Create summary statistics per `source_ip`
   - Join back to main dataset
   - Add ranking by `bytes` within each `event_type`
   - Return top 10 records per event type

4. **Time Series Analysis** (New)
   - Extract hour from timestamp
   - Group by hour and `event_type`
   - Aggregate bytes, response times, and risk scores

### 💻 **Host Information Collected**
- Hostname and platform details
- CPU information (brand, cores, frequency)
- Memory information (total, available)
- Python version and implementation
- Timestamp of benchmark run

### 📈 **Results Storage**
- Results saved to `data/benchmark_results.csv`
- Each row represents one benchmark run
- Columns include host info + timing results for each library/operation
- Supports multiple runs for comparison across different machines

## Usage

### Basic Run
```bash
python scripts/tests/benchmark.py
```

### Install Optional Dependencies (Recommended)
```bash
python scripts/install_benchmark_deps.py
```
This installs `py-cpuinfo` for detailed CPU information.

### View Results
The CSV file `data/benchmark_results.csv` contains:
- Host system information
- Timing results for each operation and library
- Can be opened in Excel, imported into analysis tools, etc.

## Sample Output

```
============================================================
COMPREHENSIVE DATA PROCESSING BENCHMARK
============================================================
Collecting host information...
Running on: MyComputer (Windows-11-10.0.26100-SP0)
CPU: Intel Core i7-12700H (16 logical cores)
Memory: 32.0 GB total

FILTER_GROUP Operation:
  Fastest: polars (0.90s)
  polars    :   0.90s (x1.0)
  duckdb    :   0.76s (x1.2)
  modin     :  10.72s (x0.1)
  pandas    :  14.23s (x0.1)

Results saved to: data/benchmark_results.csv
```

## CSV Column Structure

### Host Information Columns
- `timestamp`: When benchmark was run
- `hostname`: Computer name
- `platform`, `system`, `release`, `version`: OS details
- `machine`, `processor`: Hardware architecture
- `cpu_count_logical`, `cpu_count_physical`: CPU core counts
- `cpu_freq_max`, `cpu_freq_current`: CPU frequencies
- `memory_total_gb`, `memory_available_gb`: Memory info
- `python_version`, `python_implementation`: Python details
- `cpu_brand`, `cpu_arch`: Detailed CPU info (if py-cpuinfo installed)

### Timing Result Columns
Format: `{operation}_{library}_seconds`
- `filter_group_pandas_seconds`
- `filter_group_modin_seconds`
- `filter_group_polars_seconds`
- `filter_group_duckdb_seconds`
- `filter_group_fireducks_seconds`
- `statistics_pandas_seconds`
- ... (and so on for all operations)

## Multi-Machine Comparison

To compare performance across different machines:

1. Run the benchmark on each machine
2. The results append to the same CSV file
3. Use the host information columns to identify different machines
4. Analyze performance differences based on hardware specs

## Notes

- **FireDucks**: Only available on Linux/macOS, shows 0.00s on Windows (not actually running)
- **Modin**: May fail on large operations due to memory limits
- **Error Handling**: Failed operations show "N/A" in results
- **Reproducibility**: Multiple runs on same machine help identify performance variance

## Extending the Benchmark

To add new operations:
1. Create functions for each library (e.g., `pandas_new_operation()`)
2. Add to the `operations` dictionary in `run_all_benchmarks()`
3. The CSV will automatically include new timing columns
