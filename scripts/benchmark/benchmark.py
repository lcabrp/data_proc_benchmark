"""Original Comprehensive Data Processing Benchmark (with CLI dataset override)."""
import gc
import time
import pandas as pd
# Enable global Pandas 3.0 future defaults for optimized memory and speed
pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True
import polars as pl
import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
import argparse
import os
import psutil
from contextlib import redirect_stderr, redirect_stdout
from typing import cast, Optional
from pathlib import Path

# Add the project root to Python path for utils import
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

# Import our utility modules
from utils.config import setup_project  # noqa: E402
from utils.data_io import read_data, find_dataset, get_dataset_size  # noqa: E402
from utils.benchmark_prep import (  # noqa: E402
    PREP_COLUMNS,
    append_csv_row_with_schema,
    build_script_name,
    decide_memory_optimization,
    get_prep_csv_values,
    load_pandas_like_for_benchmark,
    print_prep_timing,
    record_prep_timing,
    reset_prep_timings,
)
from utils.duckdb_utils import DuckDBBenchmarkSource  # noqa: E402
from utils.host_info import get_host_info  # noqa: E402
from utils.pandas_benchmark_ops import complex_join_top_ranked, timeseries_hour_counts  # noqa: E402
from utils.useful_functions import optimize_df_types  # noqa: E402
from utils.platform_utils import FIREDUCKS_AVAILABLE  # noqa: E402

# Use the reusable configuration
config = setup_project()
# Defer dataset resolution to runtime (avoid side effects when imported by workers)
DATASET_PATH: Optional[Path] = None
RESULTS_CSV_PATH = config.benchmark_results_file

# Ensure results directory exists (default path; final path may be overridden at runtime)
RESULTS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

# Benchmark dataset optimization types (centralized configuration)
BENCHMARK_OPTIMIZATION_TYPES = {
    'datetime64[ns]': ['timestamp'],
    'category': ['source_ip', 'destination_ip', 'protocol', 'event_type', 
                'severity', 'user', 'status_code', 'country', 'device_type'],
    'uint32': ['bytes', 'session_id'],
    'uint16': ['response_time_ms', 'port'],
    'float32': ['risk_score'] 
}

# Current data cache - only holds one library's data at a time
_current_library_data = None
_current_library_name = None

# Global optimization settings (set by CLI arguments)
_optimize_mode = "auto"  # Options: "auto", "always", "never"
_memory_threshold_gb = 16.0
_optimization_applied = False
_prep_memory_report = "off"
_optimized_cache_mode = "off"
_optimized_cache_dir = project_root / "data" / "cache" / "optimized"
_use_csv_dtype_hints = True
_duckdb_source = DuckDBBenchmarkSource()

def _print_prep_timing(library: str, step: str, start_time: float) -> None:
    """Print elapsed time for a dataset preparation step."""
    print_prep_timing(library, step, start_time)

def should_optimize_memory() -> bool:
    """
    Determine if memory optimization should be applied based on CLI mode and system memory.
    
    Returns:
        bool: True if optimization should be applied
    """
    return decide_memory_optimization(_optimize_mode, _memory_threshold_gb).should_optimize

def load_and_optimize_for_library(library: str):
    """
    Load and optimize data for a specific library, clearing any previous data.
    This ensures only one library's data is in memory at a time.
    Memory optimization is now conditional based on system memory and CLI flags.
    
    Args:
        library: The library name ('pandas', 'polars', 'duckdb', 'fireducks')
        
    Returns:
        Optimized DataFrame in the format expected by the library
    """
    global _current_library_data, _current_library_name, DATASET_PATH, _optimization_applied
    
    # Clear previous data to free memory
    if _current_library_data is not None:
        del _current_library_data
        _current_library_data = None
        _current_library_name = None
    
    if DATASET_PATH is None:
        raise RuntimeError("DATASET_PATH is not set")
    
    print(f"\nLoading and optimizing data for {library}...")
    
    try:
        # Check if memory optimization should be applied
        optimization_decision = decide_memory_optimization(_optimize_mode, _memory_threshold_gb)
        should_optimize = optimization_decision.should_optimize
        
        # Show memory optimization decision
        if library in ["pandas", "fireducks"]:
            print(f"  {optimization_decision.message}")

        if library in ["pandas", "fireducks"]:
            df = load_pandas_like_for_benchmark(
                cast(Path, DATASET_PATH),
                library=library,
                type_map=BENCHMARK_OPTIMIZATION_TYPES,
                should_optimize=should_optimize,
                prep_memory_report=_prep_memory_report,
                optimized_cache_mode=_optimized_cache_mode,
                optimized_cache_dir=_optimized_cache_dir,
                use_dtype_hints=_use_csv_dtype_hints,
            )
            _optimization_applied = should_optimize or _optimized_cache_mode in {"read", "readwrite"}
            _current_library_data = df
            _current_library_name = library
            print(f"  {library} data loaded and ready for benchmarking")
            return df

        prep_start = time.perf_counter()

        # Read the data using existing utility function.
        step_start = time.perf_counter()
        df = read_data(cast(Path, DATASET_PATH), library=library)
        _print_prep_timing(library, "read/load", step_start)
        
        if df is None:
            print(f"  Warning: Failed to load data for {library}")
            _print_prep_timing(library, "total before failure", prep_start)
            return None
        
        # Apply optimization only for pandas-based libraries (pandas, fireducks) and when needed
        if library in ["pandas", "fireducks"] and df is not None and should_optimize:
            step_start = time.perf_counter()
            original_memory = df.memory_usage(deep=True).sum()
            _print_prep_timing(library, "deep memory before optimization", step_start)
            
            # Use existing optimize_df_types function - DRY principle!
            # Pass copy=False as positional argument
            step_start = time.perf_counter()
            optimized_df = optimize_df_types(df, BENCHMARK_OPTIMIZATION_TYPES, False)
            _print_prep_timing(library, "dtype optimization", step_start)
            
            # Calculate and report memory savings
            step_start = time.perf_counter()
            optimized_memory = optimized_df.memory_usage(deep=True).sum()
            _print_prep_timing(library, "deep memory after optimization", step_start)
            memory_reduction = (original_memory - optimized_memory) / original_memory * 100
            
            if memory_reduction > 1:
                print(f"  {library} DataFrame optimized: {memory_reduction:.1f}% memory reduction "
                      f"({original_memory/1024/1024:.1f}MB → {optimized_memory/1024/1024:.1f}MB)")
            
            df = optimized_df
            _optimization_applied = True
        elif library in ["pandas", "fireducks"] and not should_optimize:
            print(f"  {library} DataFrame loaded without memory optimization")
            _optimization_applied = False
        
        # Cache the current library's data
        _current_library_data = df
        _current_library_name = library
        print(f"  {library} data loaded and ready for benchmarking")
        _print_prep_timing(library, "total load/optimization", prep_start)
        
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
    except Exception:
        return 0.0

def clear_current_data():
    """Clear the current data to free memory with realistic expectations."""
    global _current_library_data, _current_library_name
    
    if _current_library_data is not None:
        # Get memory usage before cleanup only when prep memory reporting is enabled.
        if _prep_memory_report != "off" and hasattr(_current_library_data, 'memory_usage'):
            try:
                memory_before = _current_library_data.memory_usage(deep=_prep_memory_report == "deep").sum()
                print(f"    Releasing {memory_before/1024/1024:.1f}MB of {_current_library_name} data")
            except Exception:
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

def _get_columns_duckdb(path: Path) -> set:
    """Get column names from file using DuckDB."""
    try:
        with _duckdb_source.query(path) as (conn, expr):
            # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
            # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
            result = conn.execute(f"SELECT * FROM {expr} LIMIT 1").fetch_arrow_table()
            return set(result.column_names)
    except Exception:
        return set()

def _run_duckdb_query(sql_builder):
    """Run a DuckDB query using the configured source mode."""
    path = cast(Path, DATASET_PATH)
    with _duckdb_source.query(path) as (conn, expr):
        # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
        # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
        return conn.execute(sql_builder(expr)).fetch_arrow_table()

def _prepare_duckdb_for_benchmark() -> None:
    """Prepare DuckDB for the configured source mode."""
    if DATASET_PATH is None:
        return
    prep_start = time.perf_counter()
    elapsed = _duckdb_source.prepare(DATASET_PATH)
    if elapsed is None:
        record_prep_timing("duckdb", "total load/optimization", 0.0)
        print("  [duckdb prep] total load/optimization: 0.000s (file scan mode)")
    else:
        record_prep_timing("duckdb", "read/load cached table", elapsed)
        print(f"  [duckdb prep] read/load cached table: {elapsed:.3f}s")
        _print_prep_timing("duckdb", "total load/optimization", prep_start)

def _clear_duckdb_after_benchmark() -> None:
    """Release a cached DuckDB connection if one exists."""
    _duckdb_source.close()

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
    return _run_duckdb_query(lambda expr: f"""
            SELECT event_type, COUNT(*) as count
            FROM {expr}
            WHERE bytes > 1000
            GROUP BY event_type
        """)

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
    return _run_duckdb_query(lambda expr: f"""
            SELECT 
                event_type,
                AVG(bytes) as bytes_mean, MIN(bytes) as bytes_min, MAX(bytes) as bytes_max,
                AVG(response_time_ms) as response_time_ms_mean, MIN(response_time_ms) as response_time_ms_min, MAX(response_time_ms) as response_time_ms_max,
                AVG(risk_score) as risk_score_mean, MIN(risk_score) as risk_score_min, MAX(risk_score) as risk_score_max
            FROM {expr}
            GROUP BY event_type
        """)

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
# Task: Join with aggregated data and rank source_ip totals within each event_type
def pandas_complex():
    df = get_current_data()
    if df is None:
        return None
    return complex_join_top_ranked(df, rank_col="bytes_rank", observed=False, sort_by_rank=True)

def polars_complex():
    df = get_current_data()
    if df is None:
        return None
    
    if 'source_ip' not in df.columns or 'bytes' not in df.columns or 'event_type' not in df.columns:
        return None
    
    # Exact same operation as pandas
    summary = (df
               .group_by('source_ip')
               .agg(pl.col('bytes').sum().alias('total_bytes')))
    
    result = (df
              .join(summary, on='source_ip')
              .with_columns(
                  pl.col('total_bytes')
                    .rank(method='dense', descending=True)
                    .over('event_type')
                    .alias('bytes_rank')
              )
              .filter(pl.col('bytes_rank') <= 10)
              .sort('bytes_rank'))
    
    return result

def duckdb_complex():
    return _run_duckdb_query(lambda expr: f"""
            WITH summary AS (
                SELECT source_ip, SUM(bytes) as total_bytes
                FROM {expr}
                GROUP BY source_ip
            ), joined AS (
                SELECT d.*, s.total_bytes,
                       DENSE_RANK() OVER (
                           PARTITION BY d.event_type ORDER BY s.total_bytes DESC
                       ) as bytes_rank
                FROM {expr} d
                JOIN summary s ON d.source_ip = s.source_ip
            )
            SELECT * FROM joined 
            WHERE bytes_rank <= 10
            ORDER BY bytes_rank
        """)

def fireducks_complex():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = get_current_data()
    if df is None:
        return None
    return complex_join_top_ranked(df, rank_col="bytes_rank", observed=False, sort_by_rank=True)

# Operation 4: Time Series Analysis
# Task: Extract hour from timestamp, group by hour and event_type, count occurrences
def pandas_timeseries():
    df = get_current_data()
    if df is None:
        return None
    return timeseries_hour_counts(df, observed=False, reset_index=True, hour_name="hour")

def polars_timeseries():
    df = get_current_data()
    if df is None:
        return None
    
    if 'timestamp' not in df.columns or 'event_type' not in df.columns:
        return None
    
    timestamp_col = df['timestamp']
    if timestamp_col.dtype == pl.Utf8:
        hour_expr = pl.col('timestamp').str.slice(11, 2).cast(pl.UInt8)
    else:
        hour_expr = pl.col('timestamp').dt.hour()

    result = (df
              .with_columns(hour_expr.alias('hour'))
              .group_by(['hour', 'event_type'])
              .len()
              .rename({'len': 'count'}))
    
    return result

def duckdb_timeseries():
    return _run_duckdb_query(lambda expr: f"""
            SELECT 
                DATE_PART('hour', CAST(timestamp AS TIMESTAMP)) as hour,
                event_type,
                COUNT(*) as count
            FROM {expr}
            GROUP BY hour, event_type
            ORDER BY hour, event_type
        """)

def fireducks_timeseries():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = get_current_data()
    if df is None:
        return None
    return timeseries_hour_counts(df, observed=False, reset_index=True, hour_name="hour")

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
            print("duration: N/A")
            return None, None
        else:
            print(f"duration: {duration:.4f}s")
            return duration, result
            
    except Exception as e:
        end_time = time.perf_counter()
        duration = end_time - start_time
        print(f"ERROR after {duration:.4f}s: {str(e)[:100]}")
        return None, None

def run_pandas_fireducks_sequence(operation_definitions) -> dict:
    """
    Run pandas and fireducks consecutively sharing the same optimized DataFrame.
    This avoids duplicate optimization time when both libraries are used.
    """
    results = {}
    
    if not FIREDUCKS_AVAILABLE:
        # If FireDucks is not available, run pandas normally but without duplicate header
        pandas_data = load_and_optimize_for_library("pandas")
        if pandas_data is None:
            print("  Failed to load pandas data, skipping pandas")
            return {}
        
        # Run pandas operations
        library_operations = {}
        for op_name, op_funcs in operation_definitions:
            library_operations[op_name] = op_funcs["pandas"]
        
        pandas_results = {}
        for operation_name, operation_func in library_operations.items():
            duration, result = run_benchmark_operation("pandas", operation_func, operation_name)
            pandas_results[operation_name] = duration
            
            # Clear operation results immediately to save memory
            if result is not None:
                del result
        
        results["pandas"] = pandas_results
        
        # Clear data after pandas
        clear_current_data()
        print("  pandas data references cleared")
        
        return results
    
    # Load and optimize data once for pandas
    pandas_data = load_and_optimize_for_library("pandas")
    if pandas_data is None:
        print("  Failed to load pandas data, skipping both pandas and fireducks")
        return {}
    
    # Run pandas operations
    print("\nRunning pandas operations...")
    library_operations = {}
    for op_name, op_funcs in operation_definitions:
        library_operations[op_name] = op_funcs["pandas"]
    
    pandas_results = {}
    for operation_name, operation_func in library_operations.items():
        duration, result = run_benchmark_operation("pandas", operation_func, operation_name)
        pandas_results[operation_name] = duration
        
        # Clear operation results immediately to save memory
        if result is not None:
            del result
    
    results["pandas"] = pandas_results
    
    # Reuse the same data for fireducks without re-optimization
    print("\nReusing optimized data for fireducks (avoiding duplicate optimization)...")
    record_prep_timing("fireducks", "total load/optimization", 0.0)
    print("  [fireducks prep] reused pandas optimized data: 0.000s")
    global _current_library_name
    _current_library_name = "fireducks"  # Switch library context but keep same data
    
    # Run fireducks operations
    print("\nRunning fireducks operations...")
    library_operations = {}
    for op_name, op_funcs in operation_definitions:
        library_operations[op_name] = op_funcs["fireducks"]
    
    fireducks_results = {}
    for operation_name, operation_func in library_operations.items():
        duration, result = run_benchmark_operation("fireducks", operation_func, operation_name)
        fireducks_results[operation_name] = duration
        
        # Clear operation results immediately to save memory
        if result is not None:
            del result
    
    results["fireducks"] = fireducks_results
    
    # Clear shared data after both libraries are done
    clear_current_data()
    print("  Shared pandas/fireducks data references cleared")
    
    return results

def run_library_benchmarks(library_name: str, operations: dict) -> dict:
    """
    Run all benchmark operations for a single library.
    Load data → run operations → clear data for memory efficiency.
    """
    print(f"\n{'=' * 60}")
    print(f"BENCHMARKING {library_name.upper()}")
    print(f"{'=' * 60}")
    
    if library_name == "duckdb":
        _prepare_duckdb_for_benchmark()
    else:
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
    else:
        _clear_duckdb_after_benchmark()
    
    return library_results

# CSV Results Writing
def _write_one_results_csv(path: Path, results: dict, host_info: dict, script_name: str, dataset_size: int) -> None:
    """Write results to one CSV path, creating header if needed."""
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
        *PREP_COLUMNS,
        "script_name",
    ]

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
        *get_prep_csv_values(),
        script_name,
    ]
    append_csv_row_with_schema(path, header, row)

def write_results_to_csv(results: dict, dataset_size: int) -> None:
    """Write benchmark results to CSV file with enhanced platform detection."""
    host_info = get_host_info()  # Now includes WSL detection automatically
    
    # Create enhanced script name with optimization info (shared helper keeps all
    # benchmark runners consistent; see utils.benchmark_prep.build_script_name).
    script_name = build_script_name(
        Path(__file__).name,
        _optimize_mode,
        _optimization_applied,
    )
    
    _write_one_results_csv(RESULTS_CSV_PATH, results, host_info, script_name, dataset_size)
    print(f"\nResults written to: {RESULTS_CSV_PATH}")
    if _optimization_applied:
        print(f"Note: Memory optimization was applied (mode: {_optimize_mode}, tracking: {script_name})")
    else:
        print(f"Note: Memory optimization was skipped (mode: {_optimize_mode}, tracking: {script_name})")

def main():
    """Main function to run all benchmarks with realistic memory management."""
    global DATASET_PATH, RESULTS_CSV_PATH, _optimize_mode, _memory_threshold_gb
    global _prep_memory_report, _optimized_cache_mode, _optimized_cache_dir, _use_csv_dtype_hints

    # Simple host information display (matching benchmark_01.py style)
    print("=" * 60)
    print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
    print("=" * 60)
    
    # Get host info with enhanced platform detection
    raw_host_info = get_host_info()
    
    # Simple fallbacks for missing data
    try:
        import psutil
        if raw_host_info.get("cpu_count_logical", 0) <= 0:
            raw_host_info["cpu_count_logical"] = psutil.cpu_count(logical=True) or 0
        if raw_host_info.get("memory_total_gb", 0) <= 0:
            raw_host_info["memory_total_gb"] = psutil.virtual_memory().total / (1024**3)
    except ImportError:
        pass
    
    # Display key system information
    hostname = raw_host_info.get('hostname', 'Unknown')
    system_info = raw_host_info.get('system', 'Unknown')
    platform_info = raw_host_info.get('platform', 'Unknown')
    cpu_brand = raw_host_info.get('cpu_brand', 'Unknown')
    logical_cores = raw_host_info.get('cpu_count_logical', 0)
    memory_total = raw_host_info.get('memory_total_gb', 0.0)
    
    print(f"Running on: {hostname} ({system_info}) ({platform_info})")
    print(f"CPU: {cpu_brand} ({logical_cores} logical cores)")
    print(f"Memory: {memory_total:.2f} GB total")
    print()

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run comprehensive data processing benchmarks")
    parser.add_argument("--dataset", "-d", type=Path, help="Path to dataset file (overrides default)")
    parser.add_argument("--output", "-o", type=Path, help="Path to output CSV file (overrides default)")
    parser.add_argument("--optimize", "-opt", 
                        choices=["auto", "always", "never"], 
                        default="auto",
                        help="Memory optimization mode: 'auto' (use threshold), 'always' (force), 'never' (disable) (default: auto)")
    parser.add_argument("--mem-threshold", "-m", type=float, default=16.0, 
                        help="Memory threshold in GB for 'auto' mode (default: 16)")
    parser.add_argument("--prep-memory-report", choices=["off", "shallow", "deep"], default="off",
                        help="Memory accounting during pandas/fireducks prep. Default: off")
    parser.add_argument("--optimized-cache", choices=["off", "read", "write", "readwrite", "refresh"], default="off",
                        help="Use a Parquet cache of optimized pandas/fireducks data. Default: off")
    parser.add_argument("--optimized-cache-dir", type=Path, default=_optimized_cache_dir,
                        help="Directory for optimized pandas/fireducks cache files")
    parser.add_argument("--no-csv-dtype-hints", action="store_true",
                        help="Disable pandas/fireducks dtype hints while reading CSV input")
    parser.add_argument("--duckdb-mode", choices=["file", "cached"], default="file",
                        help="DuckDB source mode: cached mode prevents double-scanning of large files during complex operations (Optimization: 2026-05-24). Default: file")
    args = parser.parse_args()

    # Set global optimization settings
    _optimize_mode = args.optimize
    _memory_threshold_gb = args.mem_threshold
    _prep_memory_report = args.prep_memory_report
    _optimized_cache_mode = args.optimized_cache
    _optimized_cache_dir = args.optimized_cache_dir
    _use_csv_dtype_hints = not args.no_csv_dtype_hints
    _duckdb_source.set_mode(args.duckdb_mode)
    reset_prep_timings()

    # Override paths if provided via CLI
    if args.dataset:
        DATASET_PATH = args.dataset
        # Type check: DATASET_PATH is guaranteed to be a Path object here
        assert isinstance(DATASET_PATH, Path), "args.dataset should be a Path object"
        if not DATASET_PATH.exists():
            print(f"Error: Dataset file not found: {DATASET_PATH}")
            sys.exit(1)
    else:
        DATASET_PATH = find_dataset()
        if DATASET_PATH is None:
            print("Error: No dataset found. Use --dataset to specify a file.")
            sys.exit(1)

    if args.output:
        RESULTS_CSV_PATH = args.output
        RESULTS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Using dataset: {DATASET_PATH}")
    print(f"Results will be written to: {RESULTS_CSV_PATH}")
    
    # Show optimization settings
    print("Memory optimization settings:")
    if _optimize_mode == "always":
        print("  - Mode: always (forced optimization)")
    elif _optimize_mode == "never":
        print("  - Mode: never (optimization disabled)")
    else:  # auto
        print(f"  - Mode: auto (optimize if system memory < {_memory_threshold_gb}GB)")
    print(f"  - Prep memory report: {_prep_memory_report}")
    print(f"  - Optimized pandas/fireducks cache: {_optimized_cache_mode}")
    if _optimized_cache_mode != "off":
        print(f"  - Optimized cache dir: {_optimized_cache_dir}")
    print(f"  - CSV dtype hints: {'enabled' if _use_csv_dtype_hints else 'disabled'}")
    print(f"  - DuckDB mode: {args.duckdb_mode}")
    
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f}GB")

    # Get dataset size (DATASET_PATH is guaranteed to be a Path at this point)
    assert DATASET_PATH is not None, "DATASET_PATH should not be None at this point"
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

    # Run benchmarks with optimized pandas/fireducks sequence
    all_results = {}
    
    # Show appropriate header based on FireDucks availability
    if FIREDUCKS_AVAILABLE:
        header_text = "PANDAS + FIREDUCKS SEQUENCE (SHARED OPTIMIZATION)"
    else:
        header_text = "PANDAS (FireDucks not available)"
    
    print(f"\n{'=' * 60}")
    print(f"BENCHMARKING {header_text}")
    print(f"{'=' * 60}")
    
    # Run pandas and fireducks consecutively to share optimization
    pandas_fireducks_results = run_pandas_fireducks_sequence(operation_definitions)
    all_results.update(pandas_fireducks_results)
    
    # Run other libraries individually
    other_libraries = ["polars", "duckdb"]
    for library_name in other_libraries:
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
        "complex_join": "Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10",
        "timeseries": "Extract hour from timestamp, group by hour+event_type, count"
    }
    
    # Determine which libraries actually ran
    libraries_run = list(all_results.keys())
    
    # Use the same order as operation_definitions
    for operation_name, _ in operation_definitions:
        print(f"\n{operation_name.upper().replace('_', ' ')}:")
        print(f"  Task: {operation_descriptions[operation_name]}")
        
        # Collect results for this operation and sort by duration (fastest first)
        operation_results = []
        for library_name in libraries_run:
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
