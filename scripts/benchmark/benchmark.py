"""Original Comprehensive Data Processing Benchmark (with CLI dataset override)."""
import gc
import time
import pandas as pd
import polars as pl
import duckdb
import platform
import sys
import csv
import argparse
import os
from contextlib import redirect_stderr, redirect_stdout
from typing import cast, Optional
from pathlib import Path

# Add the project root to Python path for utils import
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

# Import our utility modules
from utils.config import setup_project
from utils.data_io import read_data, find_dataset, get_dataset_size
from utils.host_info import get_host_info
from utils.useful_functions import optimize_df_types

# Use the reusable configuration
config = setup_project()
# Defer dataset resolution to runtime (avoid side effects when imported by workers)
DATASET_PATH: Optional[Path] = None
RESULTS_CSV_PATH = config.benchmark_results_file

# Ensure results directory exists (default path; final path may be overridden at runtime)
RESULTS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

# FireDucks check (Linux/macOS only)
FIREDUCKS_AVAILABLE = False
if platform.system() in ["Linux", "Darwin"]:
    try:
        import fireducks.pandas as fpd  # type: ignore
        FIREDUCKS_AVAILABLE = True
    except ImportError:
        pass

# Benchmark dataset optimization types (centralized configuration)
BENCHMARK_OPTIMIZATION_TYPES = {
    'datetime64[ns]': ['timestamp'],
    'category': ['source_ip', 'destination_ip', 'protocol', 'event_type', 
                'severity', 'user', 'status_code', 'country', 'device_type'],
    'uint32': ['bytes', 'session_id', 'port'],
    'uint16': ['response_time_ms'],
    'float32': ['risk_score'] 
}

# Current data cache - only holds one library's data at a time
_current_library_data = None
_current_library_name = None

def load_and_optimize_for_library(library: str):
    """
    Load and optimize data for a specific library, clearing any previous data.
    This ensures only one library's data is in memory at a time.
    
    Args:
        library: The library name ('pandas', 'polars', 'duckdb', 'fireducks')
        
    Returns:
        Optimized DataFrame in the format expected by the library
    """
    global _current_library_data, _current_library_name, DATASET_PATH
    
    # Clear previous data to free memory
    if _current_library_data is not None:
        del _current_library_data
        _current_library_data = None
        _current_library_name = None
    
    if DATASET_PATH is None:
        raise RuntimeError("DATASET_PATH is not set")
    
    print(f"\nLoading and optimizing data for {library}...")
    
    try:
        # Read the data using existing utility function (now supports fireducks!)
        df = read_data(cast(Path, DATASET_PATH), library=library)
        
        if df is None:
            print(f"  Warning: Failed to load data for {library}")
            return None
        
        # Apply optimization only for pandas-based libraries (pandas, fireducks)
        if library in ["pandas", "fireducks"] and df is not None:
            original_memory = df.memory_usage(deep=True).sum()
            
            # Use existing optimize_df_types function - DRY principle!
            optimized_df = optimize_df_types(df, BENCHMARK_OPTIMIZATION_TYPES)
            
            # Calculate and report memory savings
            optimized_memory = optimized_df.memory_usage(deep=True).sum()
            memory_reduction = (original_memory - optimized_memory) / original_memory * 100
            
            if memory_reduction > 1:
                print(f"  {library} DataFrame optimized: {memory_reduction:.1f}% memory reduction "
                      f"({original_memory/1024/1024:.1f}MB → {optimized_memory/1024/1024:.1f}MB)")
            
            df = optimized_df
        
        # Cache the current library's data
        _current_library_data = df
        _current_library_name = library
        print(f"  {library} data loaded and ready for benchmarking")
        
        return df
        
    except Exception as e:
        print(f"  Error loading/optimizing data for {library}: {e}")
        return None

def get_current_data():
    """Get the currently loaded library's data."""
    return _current_library_data

def get_memory_usage():
    """Get current memory usage in GB."""
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024 / 1024  # GB
    except:
        return 0.0

def clear_current_data():
    """Clear the current data to free memory with realistic expectations."""
    global _current_library_data, _current_library_name
    
    if _current_library_data is not None:
        # Get memory usage before cleanup (for reporting)
        if hasattr(_current_library_data, 'memory_usage'):
            try:
                memory_before = _current_library_data.memory_usage(deep=True).sum()
                print(f"    Releasing {memory_before/1024/1024:.1f}MB of {_current_library_name} data")
            except:
                print(f"    Releasing {_current_library_name} data from memory")
        else:
            print(f"    Releasing {_current_library_name} data from memory")
        
        # Explicit deletion
        del _current_library_data
        _current_library_data = None
        _current_library_name = None
        
        # Single garbage collection pass (being realistic about what GC can do)
        gc.collect()
        
        # Brief delay for cleanup
        time.sleep(0.2)

# Helper function to find first present column
def _first_present(cols, candidates):
    """Return the first column from candidates that exists in cols."""
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None

# Helper functions for DuckDB
def _duckdb_table_expr(path: Path) -> str:
    """Generate DuckDB table expression for file."""
    if path.suffix.lower() == '.parquet':
        return f"'{path}'"
    elif path.suffix.lower() == '.csv':
        return f"read_csv_auto('{path}')"
    else:
        return f"'{path}'"

def _get_columns_duckdb(path: Path) -> set:
    """Get column names from file using DuckDB."""
    conn = duckdb.connect()
    try:
        expr = _duckdb_table_expr(path)
        result = conn.execute(f"SELECT * FROM {expr} LIMIT 1").fetchdf()
        return set(result.columns)
    except Exception:
        return set()
    finally:
        conn.close()

# STANDARDIZED BENCHMARK OPERATIONS
# Each operation performs the exact same logical steps across all libraries

# Operation 1: Filter and Group
# Task: Filter rows where bytes > 1000, then group by event_type and count rows
def pandas_filter_group():
    df = get_current_data()
    if df is None:
        return None
    # Standardized operation: filter bytes > 1000, group by event_type, count
    if 'bytes' not in df.columns or 'event_type' not in df.columns:
        return None
    filtered = df[df['bytes'] > 1000]
    return filtered.groupby('event_type').size().reset_index(name='count')

def polars_filter_group():
    df = get_current_data()
    if df is None:
        return None
    # Exact same operation as pandas
    if 'bytes' not in df.columns or 'event_type' not in df.columns:
        return None
    return (df
            .filter(pl.col('bytes') > 1000)
            .group_by('event_type')
            .len()
            .rename({'len': 'count'}))

def duckdb_filter_group():
    conn = duckdb.connect()
    try:
        path = cast(Path, DATASET_PATH)
        expr = _duckdb_table_expr(path)
        # Exact same operation as pandas/polars
        return conn.execute(f"""
            SELECT event_type, COUNT(*) as count
            FROM {expr}
            WHERE bytes > 1000
            GROUP BY event_type
        """).fetchdf()
    finally:
        conn.close()

def fireducks_filter_group():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = get_current_data()
    if df is None:
        return None
    # Exact same operation as pandas
    if 'bytes' not in df.columns or 'event_type' not in df.columns:
        return None
    filtered = df[df['bytes'] > 1000]
    return filtered.groupby('event_type').size().reset_index(name='count')

# Operation 2: Statistical Analysis  
# Task: Group by event_type and calculate mean, min, max for bytes, response_time_ms, risk_score
def pandas_stats():
    df = get_current_data()
    if df is None:
        return None
    # Standardized columns for statistics
    stat_cols = ['bytes', 'response_time_ms', 'risk_score']
    available_cols = [col for col in stat_cols if col in df.columns and df[col].dtype.kind in 'biufc']
    
    if not available_cols or 'event_type' not in df.columns:
        return None
    
    return df.groupby('event_type')[available_cols].agg(['mean', 'min', 'max']).reset_index()

def polars_stats():
    df = get_current_data()
    if df is None:
        return None
    # Exact same operation as pandas
    stat_cols = ['bytes', 'response_time_ms', 'risk_score']
    available_cols = [col for col in stat_cols if col in df.columns]
    
    if not available_cols or 'event_type' not in df.columns:
        return None
    
    aggs = []
    for col in available_cols:
        aggs.extend([
            pl.col(col).mean().alias(f'{col}_mean'),
            pl.col(col).min().alias(f'{col}_min'),
            pl.col(col).max().alias(f'{col}_max')
        ])
    
    return df.group_by('event_type').agg(aggs)

def duckdb_stats():
    conn = duckdb.connect()
    try:
        path = cast(Path, DATASET_PATH)
        expr = _duckdb_table_expr(path)
        # Exact same operation as pandas/polars
        return conn.execute(f"""
            SELECT 
                event_type,
                AVG(bytes) as bytes_mean, MIN(bytes) as bytes_min, MAX(bytes) as bytes_max,
                AVG(response_time_ms) as response_time_ms_mean, MIN(response_time_ms) as response_time_ms_min, MAX(response_time_ms) as response_time_ms_max,
                AVG(risk_score) as risk_score_mean, MIN(risk_score) as risk_score_min, MAX(risk_score) as risk_score_max
            FROM {expr}
            GROUP BY event_type
        """).fetchdf()
    finally:
        conn.close()

def fireducks_stats():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = get_current_data()
    if df is None:
        return None
    # Exact same operation as pandas
    stat_cols = ['bytes', 'response_time_ms', 'risk_score']
    available_cols = [col for col in stat_cols if col in df.columns and df[col].dtype.kind in 'biufc']
    
    if not available_cols or 'event_type' not in df.columns:
        return None
    
    return df.groupby('event_type')[available_cols].agg(['mean', 'min', 'max']).reset_index()

# Operation 3: Complex Join and Window Functions
# Task: Join with aggregated data and rank by total bytes per source_ip
def pandas_complex():
    df = get_current_data()
    if df is None:
        return None
    
    if 'source_ip' not in df.columns or 'bytes' not in df.columns:
        return None
    
    # Step 1: Create summary by source_ip
    summary = df.groupby('source_ip')['bytes'].sum().reset_index()
    summary.rename(columns={'bytes': 'total_bytes'}, inplace=True)
    
    # Step 2: Join back to original data
    result = df.merge(summary, on='source_ip')
    
    # Step 3: Add rank by total_bytes (descending)
    result['bytes_rank'] = result['total_bytes'].rank(method='dense', ascending=False)
    
    # Step 4: Return top 10 ranks only
    return result[result['bytes_rank'] <= 10].sort_values('bytes_rank')

def polars_complex():
    df = get_current_data()
    if df is None:
        return None
    
    if 'source_ip' not in df.columns or 'bytes' not in df.columns:
        return None
    
    # Exact same operation as pandas
    summary = (df
               .group_by('source_ip')
               .agg(pl.col('bytes').sum().alias('total_bytes')))
    
    result = (df
              .join(summary, on='source_ip')
              .with_columns(
                  pl.col('total_bytes').rank(method='dense', descending=True).alias('bytes_rank')
              )
              .filter(pl.col('bytes_rank') <= 10)
              .sort('bytes_rank'))
    
    return result

def duckdb_complex():
    conn = duckdb.connect()
    try:
        path = cast(Path, DATASET_PATH)
        expr = _duckdb_table_expr(path)
        # Exact same operation as pandas/polars
        return conn.execute(f"""
            WITH summary AS (
                SELECT source_ip, SUM(bytes) as total_bytes
                FROM {expr}
                GROUP BY source_ip
            ), joined AS (
                SELECT d.*, s.total_bytes,
                       DENSE_RANK() OVER (ORDER BY s.total_bytes DESC) as bytes_rank
                FROM {expr} d
                JOIN summary s ON d.source_ip = s.source_ip
            )
            SELECT * FROM joined 
            WHERE bytes_rank <= 10
            ORDER BY bytes_rank
        """).fetchdf()
    finally:
        conn.close()

def fireducks_complex():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = get_current_data()
    if df is None:
        return None
    
    if 'source_ip' not in df.columns or 'bytes' not in df.columns:
        return None
    
    # Exact same operation as pandas
    summary = df.groupby('source_ip')['bytes'].sum().reset_index()
    summary.rename(columns={'bytes': 'total_bytes'}, inplace=True)
    
    result = df.merge(summary, on='source_ip')
    result['bytes_rank'] = result['total_bytes'].rank(method='dense', ascending=False)
    
    return result[result['bytes_rank'] <= 10].sort_values('bytes_rank')

# Operation 4: Time Series Analysis
# Task: Extract hour from timestamp, group by hour and event_type, count occurrences
def pandas_timeseries():
    df = get_current_data()
    if df is None:
        return None
    
    if 'timestamp' not in df.columns or 'event_type' not in df.columns:
        return None
    
    df_copy = df.copy()  # Don't modify cached data
    df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'], errors='coerce')
    df_copy['hour'] = df_copy['timestamp'].dt.hour
    
    return df_copy.groupby(['hour', 'event_type']).size().reset_index(name='count')

def polars_timeseries():
    df = get_current_data()
    if df is None:
        return None
    
    if 'timestamp' not in df.columns or 'event_type' not in df.columns:
        return None
    
    # Exact same operation as pandas
    result = (df
              .with_columns(
                  pl.col('timestamp').str.strptime(pl.Datetime, strict=False).dt.hour().alias('hour')
              )
              .group_by(['hour', 'event_type'])
              .len()
              .rename({'len': 'count'}))
    
    return result

def duckdb_timeseries():
    conn = duckdb.connect()
    try:
        path = cast(Path, DATASET_PATH)
        expr = _duckdb_table_expr(path)
        # Exact same operation as pandas/polars
        return conn.execute(f"""
            SELECT 
                DATE_PART('hour', CAST(timestamp AS TIMESTAMP)) as hour,
                event_type,
                COUNT(*) as count
            FROM {expr}
            GROUP BY hour, event_type
            ORDER BY hour, event_type
        """).fetchdf()
    finally:
        conn.close()

def fireducks_timeseries():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = get_current_data()
    if df is None:
        return None
    
    if 'timestamp' not in df.columns or 'event_type' not in df.columns:
        return None
    
    # Exact same operation as pandas
    df_copy = df.copy()  # Don't modify cached data
    df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'], errors='coerce')
    df_copy['hour'] = df_copy['timestamp'].dt.hour
    
    return df_copy.groupby(['hour', 'event_type']).size().reset_index(name='count')

# Benchmark execution
def run_benchmark_operation(library_name, operation_func, operation_name):
    """Run a single benchmark operation with timing and error handling."""
    print(f"  {library_name} {operation_name}...", end=" ", flush=True)
    start_time = time.perf_counter()
    
    try:
        with redirect_stdout(open(os.devnull, 'w')), redirect_stderr(open(os.devnull, 'w')):
            result = operation_func()
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        
        if result is None:
            print(f"duration: N/A")
            return None, None
        else:
            print(f"duration: {duration:.4f}s")
            return duration, result
            
    except Exception as e:
        end_time = time.perf_counter()
        duration = end_time - start_time
        print(f"ERROR after {duration:.4f}s: {str(e)[:100]}")
        return None, None

def run_library_benchmarks(library_name: str, operations: dict) -> dict:
    """
    Run all benchmark operations for a single library.
    Load data → run operations → clear data for memory efficiency.
    """
    print(f"\n{'=' * 60}")
    print(f"BENCHMARKING {library_name.upper()}")
    print(f"{'=' * 60}")
    
    # Special case for DuckDB - it reads directly from file, no need to load in memory
    if library_name != "duckdb":
        data = load_and_optimize_for_library(library_name)
        if data is None and library_name != "fireducks":  # fireducks returns None on Windows, that's OK
            print(f"  Skipping {library_name} - failed to load data")
            return {}
    
    # Run all operations for this library
    library_results = {}
    for operation_name, operation_func in operations.items():
        duration, result = run_benchmark_operation(library_name, operation_func, operation_name)
        library_results[operation_name] = duration
        
        # Clear operation results immediately to save memory
        if result is not None:
            del result
    
    # Clear data after each library (but don't expect miracles)
    if library_name != "duckdb":
        clear_current_data()
        print(f"  {library_name} data references cleared")
    
    return library_results

# CSV Results Writing
def _write_one_results_csv(path: Path, results: dict, host_info: dict, script_name: str, dataset_size: int) -> None:
    """Write results to one CSV path, creating header if needed."""
    file_exists = path.exists()
    with open(path, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if not file_exists:
                header = [
                    "timestamp", "hostname", "platform", "system", "release", "version", "machine", "processor",
                    "cpu_count_logical", "cpu_count_physical", "cpu_freq_max", "cpu_freq_current",
                    "memory_total_gb", "memory_available_gb", "python_version", "python_implementation",
                    "cpu_brand", "cpu_arch",
                    "dataset_size", "dataset_name", "dataset_format",
                    "filter_group_pandas_seconds", "filter_group_polars_seconds",
                    "filter_group_duckdb_seconds", "filter_group_fireducks_seconds",
                    "statistics_pandas_seconds", "statistics_polars_seconds",
                    "statistics_duckdb_seconds", "statistics_fireducks_seconds",
                    "complex_join_pandas_seconds", "complex_join_polars_seconds",
                    "complex_join_duckdb_seconds", "complex_join_fireducks_seconds",
                    "timeseries_pandas_seconds", "timeseries_polars_seconds",
                    "timeseries_duckdb_seconds", "timeseries_fireducks_seconds",
                    "script_name",
                ]
                writer.writerow(header)

            # Derive dataset metadata
            try:
                ds_name = DATASET_PATH.name if DATASET_PATH else 'unknown'
                suffs = [s.lower() for s in (DATASET_PATH.suffixes if DATASET_PATH else [])]
                comp = {'.gz', '.zip', '.zst', '.bz2'}
                base = [s for s in suffs if s not in comp]
                ext = (base[-1] if base else (DATASET_PATH.suffix if DATASET_PATH else '')).lower().lstrip('.')
                if ext in ('jsonl', 'ndjson'):
                    ext = 'ndjson'
                ds_fmt = ext or 'unknown'
            except Exception:
                ds_name = 'unknown'
                ds_fmt = 'unknown'

            row = [
                host_info.get("timestamp"), host_info.get("hostname"), host_info.get("platform"),
                host_info.get("system"), host_info.get("release"), host_info.get("version"),
                host_info.get("machine"), host_info.get("processor"), host_info.get("cpu_count_logical"),
                host_info.get("cpu_count_physical"), host_info.get("cpu_freq_max"), host_info.get("cpu_freq_current"),
                host_info.get("memory_total_gb"), host_info.get("memory_available_gb"), host_info.get("python_version"),
                host_info.get("python_implementation"), host_info.get("cpu_brand"), host_info.get("cpu_arch"),
                dataset_size, ds_name, ds_fmt,
                results.get("pandas", {}).get("filter_group"),
                results.get("polars", {}).get("filter_group"),
                results.get("duckdb", {}).get("filter_group"),
                results.get("fireducks", {}).get("filter_group"),
                results.get("pandas", {}).get("statistics"),
                results.get("polars", {}).get("statistics"),
                results.get("duckdb", {}).get("statistics"),
                results.get("fireducks", {}).get("statistics"),
                results.get("pandas", {}).get("complex_join"),
                results.get("polars", {}).get("complex_join"),
                results.get("duckdb", {}).get("complex_join"),
                results.get("fireducks", {}).get("complex_join"),
                results.get("pandas", {}).get("timeseries"),
                results.get("polars", {}).get("timeseries"),
                results.get("duckdb", {}).get("timeseries"),
                results.get("fireducks", {}).get("timeseries"),
                script_name,
            ]
            writer.writerow(row)

def write_results_to_csv(results: dict, dataset_size: int) -> None:
    """Write benchmark results to CSV file."""
    host_info = get_host_info()
    script_name = Path(__file__).name
    _write_one_results_csv(RESULTS_CSV_PATH, results, host_info, script_name, dataset_size)
    print(f"\nResults written to: {RESULTS_CSV_PATH}")

# Main benchmark execution
def main():
    """Main function to run all benchmarks with realistic memory management."""
    global DATASET_PATH, RESULTS_CSV_PATH

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run comprehensive data processing benchmarks")
    parser.add_argument("--dataset", "-d", type=Path, help="Path to dataset file (overrides default)")
    parser.add_argument("--output", "-o", type=Path, help="Path to output CSV file (overrides default)")
    args = parser.parse_args()

    # Override paths if provided via CLI
    if args.dataset:
        DATASET_PATH = args.dataset
        if not DATASET_PATH.exists():
            print(f"Error: Dataset file not found: {DATASET_PATH}")
            sys.exit(1)
    else:
        DATASET_PATH = find_dataset()
        if not DATASET_PATH:
            print("Error: No dataset found. Use --dataset to specify a file.")
            sys.exit(1)

    if args.output:
        RESULTS_CSV_PATH = args.output
        RESULTS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Using dataset: {DATASET_PATH}")
    print(f"Results will be written to: {RESULTS_CSV_PATH}")
    
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f}GB")

    # Get dataset size
    dataset_size = get_dataset_size(DATASET_PATH)
    print(f"Dataset size: {dataset_size:,} rows")

    # Define all operations for each library in ORDERED fashion
    operation_definitions = [
        ("filter_group", {
            "pandas": pandas_filter_group,
            "polars": polars_filter_group,
            "duckdb": duckdb_filter_group,
            "fireducks": fireducks_filter_group,
        }),
        ("statistics", {
            "pandas": pandas_stats,
            "polars": polars_stats,
            "duckdb": duckdb_stats,
            "fireducks": fireducks_stats,
        }),
        ("complex_join", {
            "pandas": pandas_complex,
            "polars": polars_complex,
            "duckdb": duckdb_complex,
            "fireducks": fireducks_complex,
        }),
        ("timeseries", {
            "pandas": pandas_timeseries,
            "polars": polars_timeseries,
            "duckdb": duckdb_timeseries,
            "fireducks": fireducks_timeseries,
        }),
    ]

    # Run benchmarks for each library sequentially
    all_results = {}
    libraries = ["pandas", "polars", "duckdb"]
    if FIREDUCKS_AVAILABLE:
        libraries.append("fireducks")

    for library_name in libraries:
        # Show memory usage before each library
        mem_before = get_memory_usage()
        print(f"\nMemory usage before {library_name}: {mem_before:.2f}GB")
        
        # Create ordered operations dict for this library
        library_operations = {}
        for op_name, op_funcs in operation_definitions:
            library_operations[op_name] = op_funcs[library_name]
        
        library_results = run_library_benchmarks(library_name, library_operations)
        all_results[library_name] = library_results
        
        # Show memory usage after cleanup
        mem_after = get_memory_usage()
        print(f"Memory usage after {library_name}: {mem_after:.2f}GB")

    # Final memory usage
    final_memory = get_memory_usage()
    print(f"\nFinal memory usage: {final_memory:.2f}GB")
    
    # Memory usage information (not a warning, just information)
    memory_growth = final_memory - initial_memory
    if memory_growth > 1.0:
        print(f"📊 Memory growth: {memory_growth:.2f}GB")
        print("   This is normal for data processing libraries that use:")
        print("   • Memory mapping for large files")
        print("   • Internal caches for performance")
        print("   • Thread pools and connection pooling")
        print("   • Optimized data structures that persist")

    # Write results to CSV
    write_results_to_csv(all_results, dataset_size)

    # Print summary with operation descriptions, SORTED BY PERFORMANCE (fastest first)
    print(f"\n{'=' * 70}")
    print("BENCHMARK SUMMARY (sorted by performance - fastest first)")
    print(f"{'=' * 70}")
    
    operation_descriptions = {
        "filter_group": "Filter bytes > 1000, group by event_type, count rows",
        "statistics": "Group by event_type, calc mean/min/max for bytes/response_time_ms/risk_score",
        "complex_join": "Sum bytes by source_ip, join back, rank by total_bytes, top 10",
        "timeseries": "Extract hour from timestamp, group by hour+event_type, count"
    }
    
    # Use the same order as operation_definitions
    for operation_name, _ in operation_definitions:
        print(f"\n{operation_name.upper().replace('_', ' ')}:")
        print(f"  Task: {operation_descriptions[operation_name]}")
        
        # Collect results for this operation and sort by duration (fastest first)
        operation_results = []
        for library_name in libraries:
            duration = all_results.get(library_name, {}).get(operation_name)
            if duration is not None:
                operation_results.append((library_name, duration))
            else:
                operation_results.append((library_name, float('inf')))  # Put N/A at the end
        
        # Sort by duration (fastest first, N/A last)
        operation_results.sort(key=lambda x: x[1])
        
        # Display sorted results
        for rank, (library_name, duration) in enumerate(operation_results, 1):
            if duration == float('inf'):
                print(f"    {rank}. {library_name:10}: N/A")
            else:
                speedup_text = ""
                if rank > 1:
                    fastest_time = operation_results[0][1]
                    if fastest_time > 0:
                        speedup = duration / fastest_time
                        speedup_text = f" ({speedup:.1f}x slower)"
                
                print(f"    {rank}. {library_name:10}: {duration:.4f}s{speedup_text}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Critical error in main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Simple cleanup in finally block
        print("\nCleaning up...")
        clear_current_data()
        print("Benchmark completed.")