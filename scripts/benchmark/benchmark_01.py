"""
Comprehensive Data Processing Benchmark Script

Benchmarks multiple data processing libraries (pandas, modin, polars, duckdb, fireducks) on several operations.
Allows configuration of repeat count via command-line argument (default: 1).
Uses best software engineering practices, type hints, and docstrings.
"""

import time
from typing import Optional, Callable, Any, Union, List, TypeVar, cast
import pandas
import modin.pandas
import polars
import gzip
import json
import zipfile
from pathlib import Path
import numpy as np

# Define specialized DataFrame types for type checking
PandasDataFrame = pandas.DataFrame
ModinDataFrame = modin.pandas.DataFrame 
PolarsDataFrame = polars.DataFrame
import pandas as pd
import modin.pandas as mpd
import polars as pl
import duckdb
import modin.config as modin_cfg
import logging
import warnings
import sys
import os
import psutil
from distributed import Client
from pandas.core.frame import DataFrame as PandasDF
from modin.pandas.dataframe import DataFrame as ModinDF
from polars.dataframe import DataFrame as PolarsDF

AnyDataFrame = Union[PandasDF, ModinDF, PolarsDF]
import csv
import gc
from datetime import datetime
from contextlib import redirect_stderr
from dask.distributed import Client, LocalCluster
import argparse
import sys
import os
import pathlib
from pathlib import Path

# Suppress noisy SyntaxWarnings (e.g. invalid escape sequence '\_') that originate
# from third-party packages or docstrings not affecting runtime behavior.
warnings.filterwarnings("ignore", category=SyntaxWarning, message=r"invalid escape sequence '\\_'")

# Add the project root to Python path for utils import
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Import our utility modules
from utils import (
    get_host_info, get_memory_usage_mb, log_memory_usage,
    PlatformDetector, SystemInfo
)
from utils.data_io import UniversalDataReader, get_dataset_size as universal_dataset_size

# Get platform flags and library availability from utils
platform_flags = PlatformDetector.get_platform_flags()
IS_WINDOWS = platform_flags['IS_WINDOWS']
IS_WSL = platform_flags['IS_WSL'] 
IS_LINUX = platform_flags['IS_LINUX']
IS_MACOS = platform_flags['IS_MACOS']

# Defer library availability check to avoid potential hanging imports
RAY_AVAILABLE = False
FIREDUCKS_AVAILABLE = False

# Import optional libraries if available - done lazily in functions
# if RAY_AVAILABLE:
#     import ray
# if FIREDUCKS_AVAILABLE:
#     import fireducks.pandas as fpd

# Default paths
CSV_PATH = Path("data/raw/synthetic_logs_test.csv")
RESULTS_CSV_PATH = Path("data/benchmark_results.csv")

# Ensure directories exist
CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
RESULTS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

# Modin engine configuration - defer to avoid potential import issues
DEFAULT_MODIN_ENGINE = "dask"  # Safe default, will be set properly in setup_modin()

_universal_reader = UniversalDataReader(default_library='pandas')

def get_dataset_size(csv_path: str) -> int:
    """Get number of records (supports csv/parquet/json/ndjson, compressed variants)."""
    try:
        return universal_dataset_size(Path(csv_path))
    except Exception as e:
        print(f"Warning: Could not determine dataset size: {e}")
        return 0

def _detect_format(path: Union[str, Path]) -> str:
    """Detect file format using UniversalDataReader logic (csv, parquet, json, ndjson)."""
    try:
        return _universal_reader.detect_file_format(Path(path))
    except Exception:
        p = Path(path)
        suffs = [s for s in p.suffixes if s.lower() not in {'.gz', '.zip', '.zst', '.bz2'}]
        if suffs:
            ext = suffs[-1].lower()
            if ext in ('.jsonl', '.ndjson'): return 'ndjson'
            if ext == '.json': return 'json'
            if ext == '.parquet': return 'parquet'
        return 'csv'

def _read_pandas(path: str) -> PandasDataFrame:
    fmt = _detect_format(path)
    if fmt == 'parquet':
        return cast(PandasDataFrame, pd.read_parquet(path))
    if fmt == 'json':
        return cast(PandasDataFrame, pd.read_json(path))
    if fmt == 'ndjson':
        return cast(PandasDataFrame, pd.read_json(path, lines=True))
    return cast(PandasDataFrame, pd.read_csv(path))

def _read_modin(path: str) -> ModinDataFrame:
    fmt = _detect_format(path)
    if fmt == 'parquet':
        return cast(ModinDataFrame, mpd.read_parquet(path))
    if fmt == 'json':
        return cast(ModinDataFrame, mpd.read_json(path))
    if fmt == 'ndjson':
        return cast(ModinDataFrame, mpd.read_json(path, lines=True))
    return cast(ModinDataFrame, mpd.read_csv(path))

def _polars_lazy(path: str):
    fmt = _detect_format(path)
    if fmt == 'csv':
        return pl.scan_csv(path)
    if fmt == 'parquet':
        return pl.scan_parquet(path)
    if fmt == 'json':
        # no lazy reader; load eagerly then convert
        return pl.read_json(path).lazy()
    if fmt == 'ndjson':
        try:
            return pl.read_ndjson(path).lazy()
        except Exception:
            return pl.read_json(path, lines=True).lazy()
    return pl.scan_csv(path)

def _duckdb_source(path: str) -> str:
    fmt = _detect_format(path)
    if fmt == 'parquet':
        return f"read_parquet('{path}')"
    if fmt in ('json', 'ndjson'):
        return f"read_json_auto('{path}')"
    return f"read_csv_auto('{path}')"

def setup_modin() -> None:
    """Initialize Modin with environment-appropriate configuration.

    Keep configuration minimal so Modin/Dask can utilize all available cores
    similar to the original `benchmark.py` behavior.
    """
    # Get the recommended engine dynamically
    engine = PlatformDetector.get_recommended_modin_engine()
    modin_cfg.Engine.put(engine)
    logging.getLogger("distributed").setLevel(logging.CRITICAL)
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    warnings.filterwarnings("ignore")
    
    # Initialize Ray if using Ray engine
    if engine == "ray" and RAY_AVAILABLE:
        try:
            import ray
            if not ray.is_initialized():
                ray.init(ignore_reinit_error=True)
                print(f"Ray initialized for Modin")
        except Exception as e:
            print(f"Warning: Failed to initialize Ray: {e}")
    
    # Reset adaptive Modin failure tracking on fresh setup
    globals()['__modin_failure_count'] = 0
    globals()['__modin_failure_threshold'] = 3
    globals()['__modin_disabled'] = False
    print(f"Modin initialized with {engine} engine")

def get_dask_client() -> Optional[Client]:
    """
    Get environment-appropriate Dask client configuration.
    Returns Optional[Client] - None if using Ray or if client creation fails.
    """
    try:
        if DEFAULT_MODIN_ENGINE == "ray":
            return None  # Ray doesn't need a client

        total_memory = psutil.virtual_memory().total
        total_gb = total_memory // (1024 ** 3)
        logical = psutil.cpu_count(logical=True) or 4

        # On Windows prefer a threaded LocalCluster with fewer workers
        # to reduce inter-process IPC and lost-dependency issues.
        if IS_WINDOWS:
            # Use up to 2 workers on Windows to increase per-worker headroom
            n_workers = min(2, max(1, logical))
            threads_per_worker = max(1, logical // n_workers)
            cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
                processes=False,
                silence_logs=logging.CRITICAL,
            )
            return Client(cluster)

        # Prefer a per-worker memory target to avoid tasks being cancelled
        # because input dependencies exceed the worker memory limit on non-Windows.
        target_per_worker_gb = 4
        # Choose number of workers such that each worker has roughly
        # target_per_worker_gb RAM, but do not exceed logical cores.
        n_workers_by_mem = max(1, int(total_gb // target_per_worker_gb))
        n_workers = min(logical, n_workers_by_mem)

        # Ensure at least one worker
        n_workers = max(1, n_workers)

        # Distribute logical cores across workers
        threads_per_worker = max(1, logical // n_workers)

        # Create cluster without forcing a memory_limit per worker; by reducing
        # the number of workers we increase per-worker available memory.
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            processes=True,
            silence_logs=logging.CRITICAL,
        )
        return Client(cluster)
    except Exception as e:
        logging.warning(f"Failed to create Dask client: {e}")
        return None

def run_pandas_operation(func: Callable[[PandasDataFrame], Any], csv_path: str) -> Any:
    """Run operation using Pandas DataFrame (format-aware)."""
    df = _read_pandas(csv_path)
    return func(df)


def run_modin_operation(func: Callable[[ModinDataFrame], Any], csv_path: str) -> Any:
    """Run operation using Modin DataFrame with enhanced error handling.

    Tries to run with the global Dask client (created in `main()`) or a
    cluster from `get_dask_client()`. If the operation fails (for example
    due to 'lost dependencies'), it retries using a single-worker,
    threaded LocalCluster which is more stable on Windows.
    """
    # Adaptive failure tracking globals
    failure_count = globals().get('__modin_failure_count', 0)
    failure_threshold = globals().get('__modin_failure_threshold', 3)
    modin_disabled = globals().get('__modin_disabled', False)

    if modin_disabled:
        print("Modin disabled due to repeated failures; skipping Modin operation.")
        return None

    client = globals().get('client') or get_dask_client()
    try:
        df = _read_modin(csv_path)
        if client is None:
            return func(df)
        try:
            client.run(gc.collect)
        except Exception:
            pass
        result = func(df)
        if hasattr(result, '_to_pandas'):
            try:
                _ = result._to_pandas()
            except Exception:
                pass
        return result
    except Exception as e:
        print(f"Modin operation failed: {str(e)}")

        # Try to gather lightweight diagnostics
        try:
            if client is not None:
                info = client.scheduler_info()
                workers = info.get('workers', {})
                print(f"Dask scheduler workers: {len(workers)}")
                # Print per-worker memory diagnostics where available
                for addr, winfo in workers.items():
                    mem_limit = winfo.get('memory_limit')
                    mem = winfo.get('memory') or winfo.get('metrics', {}).get('memory')
                    print(f" - worker {addr}: memory_limit={mem_limit}, memory_used={mem}")
        except Exception:
            pass

        # Increment failure counter and consider disabling Modin after threshold
        failure_count += 1
        globals()['__modin_failure_count'] = failure_count
        if failure_count >= failure_threshold:
            globals()['__modin_disabled'] = True
            print(f"Modin has failed {failure_count} times; further Modin attempts will be skipped for this session.")

        # Attempt a single fallback run (single-worker threaded) only once per failure
        try:
            print("Retrying Modin operation with single-worker fallback cluster...")
            logical = psutil.cpu_count(logical=True) or 4
            threads = max(1, logical - 1)
            fallback_cluster = LocalCluster(
                n_workers=1,
                threads_per_worker=threads,
                processes=False,
                silence_logs=logging.CRITICAL,
            )
            with Client(fallback_cluster) as fb_client:
                gc.collect()
                df = _read_modin(csv_path)
                result = func(df)
                if hasattr(result, '_to_pandas'):
                    _ = result._to_pandas()
                return result
        except Exception as e2:
            print(f"Modin fallback failed: {str(e2)}")
            # Track fallback failure as well
            failure_count = globals().get('__modin_failure_count', 0) + 1
            globals()['__modin_failure_count'] = failure_count
            if failure_count >= failure_threshold:
                globals()['__modin_disabled'] = True
                print(f"Modin has failed {failure_count} times (including fallbacks); disabling further attempts.")
            return None
    finally:
        gc.collect()


def run_polars_operation(func: Callable[[PolarsDataFrame], Any], csv_path: str) -> Any:
    """Run operation using Polars DataFrame (format-aware)."""
    fmt = _detect_format(csv_path)
    if fmt == 'csv':
        df = cast(PolarsDataFrame, pl.read_csv(csv_path))
    elif fmt == 'parquet':
        df = cast(PolarsDataFrame, pl.read_parquet(csv_path))
    elif fmt == 'json':
        df = cast(PolarsDataFrame, pl.read_json(csv_path))
    elif fmt == 'ndjson':
        try:
            df = cast(PolarsDataFrame, pl.read_ndjson(csv_path))
        except Exception:
            # Fallback: read line-delimited JSON manually
            with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                import json as _json
                records = [_json.loads(line) for line in f if line.strip()]
            df = cast(PolarsDataFrame, pl.from_dicts(records))
    else:
        df = cast(PolarsDataFrame, pl.read_csv(csv_path))
    return func(df)


def run_duckdb_operation(func: Callable[[PandasDataFrame], Any], csv_path: str) -> Any:
    """Run operation using DuckDB (format-aware, converts to pandas DataFrame)."""
    fmt = _detect_format(csv_path)
    conn = duckdb.connect()
    try:
        if fmt == 'parquet':
            df = cast(PandasDataFrame, conn.read_parquet(csv_path).df())
        elif fmt in ('json', 'ndjson'):
            df = cast(PandasDataFrame, conn.execute(f"SELECT * FROM read_json_auto('{csv_path}')").fetchdf())
        else:
            df = cast(PandasDataFrame, conn.read_csv(csv_path).df())
        return func(df)
    finally:
        conn.close()


def run_benchmark_operation(
    library_name: str, 
    operation_func: Callable[[str], Union[PandasDataFrame, ModinDataFrame, PolarsDataFrame, None]], 
    operation_name: str, 
    csv_path: str
) -> Optional[float]:
    """
    Generic benchmark runner with memory logging and garbage collection.
    Args:
        library_name (str): Name of the library.
        operation_func (callable): Function to execute.
        operation_name (str): Name of the operation.
        csv_path (str): Path to CSV file.
    Returns:
        float: Duration in seconds, or None if failed.
    """
    try:
        log_memory_usage(f"{library_name} {operation_name} (start)")
        start = time.perf_counter()
        result = operation_func(csv_path)
        duration = time.perf_counter() - start
        print(f"{library_name} {operation_name} duration: {duration:.2f}s")
        log_memory_usage(f"{library_name} {operation_name} (end)")
        del result  # Explicitly delete the result
        import gc
        gc.collect()  # Force garbage collection
        # Aggressive cleanup for Modin/Dask - avoid restarting the global client
        # Unconditional restarts can kill thread-based workers (no nannies) and
        # cause persistent 'No valid workers' or restart timeouts on Windows.
        if library_name.lower() == 'modin':
            try:
                if 'client' in globals() and globals().get('client') is not None:
                    logging.info("Skipping client.restart() to preserve worker lifecycle (avoids killing non-nanny workers)")
            except Exception as e:
                logging.warning(f"Skipped restarting global Dask client due to unexpected error: {e}")
        return duration
    except Exception as e:
        print(f"{library_name} {operation_name} failed: {e}")
        log_memory_usage(f"{library_name} {operation_name} (failed)")
        import gc
        gc.collect()
        if library_name.lower() == 'modin':
            try:
                if 'client' in globals() and globals().get('client') is not None:
                    logging.info("Skipping client.restart() after failure to avoid worker lifecycle issues on Windows")
            except Exception as e2:
                logging.warning(f"Skipped restarting global Dask client after failure due to unexpected error: {e2}")
        return None

# --- Pandas Operations ---
def pandas_filter_group(csv_path: str):
    """
    Filter rows with status_code == 200 and group by source_ip, summing bytes.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Resulting DataFrame.
    """
    df = _read_pandas(csv_path)
    result = df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})
    return result

def pandas_stats(csv_path: str):
    """
    Group by event_type and compute statistics on bytes, response_time_ms, and risk_score.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Resulting DataFrame.
    """
    df = _read_pandas(csv_path)
    result = df.groupby("event_type").agg({
        "bytes": ["mean", "std", "min", "max"],
        "response_time_ms": ["mean", "median"],
        "risk_score": ["mean", "std"]
    })
    return result

def pandas_complex(csv_path: str):
    """
    Complex operation: group by source_ip, join summary, and rank bytes within event_type.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Top 10 by bytes per event_type.
    """
    df = _read_pandas(csv_path)
    summary = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"}).reset_index()
    old_cols = summary.columns.tolist()
    new_cols = ["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"]
    summary = cast(PandasDataFrame, summary.rename(columns=dict(zip(old_cols, new_cols))))
    result = df.merge(summary, on="source_ip")
    result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
    return result[result["bytes_rank"] <= 10]

def pandas_timeseries(csv_path: str):
    """
    Time series analysis: group by hour and event_type if timestamp exists, else by status_code and event_type.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Resulting DataFrame.
    """
    df = _read_pandas(csv_path)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        result = df.groupby(['hour', 'event_type']).agg({
            'bytes': ['sum', 'count'],
            'response_time_ms': 'mean',
            'risk_score': 'mean'
        })
    else:
        result = df.groupby(['status_code', 'event_type']).size().reset_index(name='count')
    return result

# --- Modin Operations ---
def modin_filter_group(csv_path: str) -> ModinDataFrame:
    """
    Modin version of filter_group operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        ModinDataFrame: Resulting DataFrame.
    """
    def _modin_filter_group(df: ModinDataFrame) -> ModinDataFrame:
        filtered = cast(ModinDataFrame, df[df["status_code"] == 200])
        grouped = cast(ModinDataFrame, filtered.groupby("source_ip").agg({"bytes": "sum"}))
        return grouped
    return cast(ModinDataFrame, run_modin_operation(_modin_filter_group, csv_path))

def modin_stats(csv_path: str) -> ModinDataFrame:
    """
    Modin version of stats operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        ModinDataFrame: Resulting DataFrame.
    """
    def _modin_stats(df: ModinDataFrame) -> ModinDataFrame:
        grouped = cast(ModinDataFrame, df.groupby("event_type").agg({
            "bytes": ["mean", "std", "min", "max"],
            "response_time_ms": ["mean", "median"],
            "risk_score": ["mean", "std"]
        }))
        return grouped
    return cast(ModinDataFrame, run_modin_operation(_modin_stats, csv_path))

def modin_complex(csv_path: str) -> ModinDataFrame:
    """
    Modin version of complex operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        ModinDataFrame: Top 10 by bytes per event_type.
    """
    def _modin_complex(df: ModinDataFrame) -> ModinDataFrame:
        grouped = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"})  # type: ignore[attr-defined]
        summary = cast(ModinDataFrame, grouped.reset_index())  # type: ignore[assignment]
        try:
            summary.columns = pd.Index(["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"])  # type: ignore[attr-defined]
        except Exception:
            pass
        result = cast(ModinDataFrame, df.merge(summary, on="source_ip"))  # type: ignore[attr-defined]
        try:
            result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)  # type: ignore[attr-defined]
        except Exception:
            return result
        return cast(ModinDataFrame, result[result["bytes_rank"] <= 10])
    return cast(ModinDataFrame, run_modin_operation(_modin_complex, csv_path))

def modin_timeseries(csv_path: str) -> ModinDataFrame:
    """
    Modin version of timeseries operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        ModinDataFrame: Resulting DataFrame.
    """
    def _modin_timeseries(df: ModinDataFrame) -> ModinDataFrame:
        # Add timestamp processing
        if 'timestamp' in df.columns:
            df_copy = cast(ModinDataFrame, df.copy())
            
            # Convert timestamp
            timestamp_series = mpd.to_datetime(df_copy['timestamp'])
            df_copy['timestamp'] = timestamp_series
            
            # Extract hour
            df_copy['hour'] = df_copy['timestamp'].dt.hour
            
            # Group and aggregate
            grouped = df_copy.groupby(['hour', 'event_type'])
            result = cast(ModinDataFrame, grouped.agg({
                'bytes': ['sum', 'count'],
                'response_time_ms': 'mean',
                'risk_score': 'mean'
            }))
            
        else:
            # Fallback to simpler grouping
            grouped = df.groupby(['status_code', 'event_type'])
            counts = cast(pd.Series, grouped.size())
            # Create DataFrame with proper index
            result = cast(ModinDataFrame, mpd.DataFrame({
                'status_code': cast(pd.Index, counts.index).get_level_values(0),
                'event_type': cast(pd.Index, counts.index).get_level_values(1),
                'count': counts.values
            }))
            
        return result
    
    return cast(ModinDataFrame, run_modin_operation(_modin_timeseries, csv_path))

# --- Polars Operations (Optimized for Memory) ---
def polars_filter_group(csv_path: str):
    """
    Polars version of filter_group operation using lazy evaluation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pl.DataFrame: Resulting DataFrame.
    """
    df = _polars_lazy(csv_path)  # Lazy evaluation
    result = (df.filter(pl.col("status_code") == 200)
               .group_by("source_ip")
               .agg(pl.sum("bytes")))
    return result.collect()

def polars_stats(csv_path: str):
    """
    Polars version of stats operation using lazy evaluation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pl.DataFrame: Resulting DataFrame.
    """
    df = _polars_lazy(csv_path)
    result = (df.group_by("event_type")
               .agg([
                   pl.col("bytes").mean().alias("bytes_mean"),
                   pl.col("bytes").std().alias("bytes_std"),
                   pl.col("bytes").min().alias("bytes_min"),
                   pl.col("bytes").max().alias("bytes_max"),
                   pl.col("response_time_ms").mean().alias("response_time_ms_mean"),
                   pl.col("response_time_ms").median().alias("response_time_ms_median"),
                   pl.col("risk_score").mean().alias("risk_score_mean"),
                   pl.col("risk_score").std().alias("risk_score_std")
               ]))
    return result.collect()

def polars_complex(csv_path: str):
    """
    Polars version of complex operation using lazy evaluation and window functions.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pl.DataFrame: Top 10 by bytes per event_type.
    """
    df = _polars_lazy(csv_path)
    summary = (df.group_by("source_ip")
                .agg([pl.col("bytes").sum().alias("total_bytes"),
                      pl.col("response_time_ms").mean().alias("avg_response_time_ms"),
                      pl.col("risk_score").mean().alias("avg_risk_score")]))
    result = (df.join(summary, on="source_ip")
               .with_columns([
                   pl.col("bytes").rank(method="dense", descending=True).over("event_type").alias("bytes_rank")
               ])
               .filter(pl.col("bytes_rank") <= 10))
    return result.collect()

def polars_timeseries(csv_path: str):
    """
    Polars version of timeseries operation using lazy evaluation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pl.DataFrame: Resulting DataFrame.
    """
    df = _polars_lazy(csv_path)
    if 'timestamp' in df.columns:
        result = (df.with_columns([
            pl.col('timestamp').str.strptime(pl.Datetime).alias('timestamp_parsed'),
        ]).with_columns([
            pl.col('timestamp_parsed').dt.hour().alias('hour')
        ]).group_by(['hour', 'event_type']).agg([
            pl.col('bytes').sum().alias('bytes_sum'),
            pl.col('bytes').count().alias('bytes_count'),
            pl.col('response_time_ms').mean().alias('response_time_ms_mean'),
            pl.col('risk_score').mean().alias('risk_score_mean')
        ]))
    else:
        result = df.group_by(['status_code', 'event_type']).len()
    return result.collect()

# --- DuckDB Operations ---
def duckdb_filter_group(csv_path: str):
    """
    DuckDB version of filter_group operation using SQL.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Resulting DataFrame.
    """
    source = _duckdb_source(csv_path)
    with duckdb.connect() as conn:
        return conn.execute(f"""
            SELECT source_ip, SUM(bytes) as bytes
            FROM {source}
            WHERE status_code = 200
            GROUP BY source_ip
        """).fetchdf()

def duckdb_stats(csv_path: str):
    """
    DuckDB version of stats operation using SQL.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Resulting DataFrame.
    """
    source = _duckdb_source(csv_path)
    with duckdb.connect() as conn:
        return conn.execute(f"""
            SELECT event_type,
                   AVG(bytes) as bytes_mean,
                   STDDEV(bytes) as bytes_std,
                   MIN(bytes) as bytes_min,
                   MAX(bytes) as bytes_max,
                   AVG(response_time_ms) as response_time_ms_mean,
                   MEDIAN(response_time_ms) as response_time_ms_median,
                   AVG(risk_score) as risk_score_mean,
                   STDDEV(risk_score) as risk_score_std
            FROM {source}
            GROUP BY event_type
        """).fetchdf()

def duckdb_complex(csv_path: str):
    """
    DuckDB version of complex operation using SQL window functions.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Top 10 by bytes per event_type.
    """
    source = _duckdb_source(csv_path)
    with duckdb.connect() as conn:
        return conn.execute(f"""
            WITH summary AS (
                SELECT source_ip,
                       SUM(bytes) as total_bytes,
                       AVG(response_time_ms) as avg_response_time_ms,
                       AVG(risk_score) as avg_risk_score
                FROM {source}
                GROUP BY source_ip
            ),
            ranked AS (
                SELECT d.*, s.total_bytes, s.avg_response_time_ms, s.avg_risk_score,
                       DENSE_RANK() OVER (PARTITION BY d.event_type ORDER BY d.bytes DESC) as bytes_rank
                FROM {source} d
                JOIN summary s ON d.source_ip = s.source_ip
            )
            SELECT * FROM ranked WHERE bytes_rank <= 10
        """).fetchdf()

def duckdb_timeseries(csv_path: str):
    """
    DuckDB version of timeseries operation using SQL.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        pd.DataFrame: Resulting DataFrame.
    """
    source = _duckdb_source(csv_path)
    with duckdb.connect() as conn:
        try:
            return conn.execute(f"""
                SELECT EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) as hour,
                       event_type,
                       SUM(bytes) as bytes_sum,
                       COUNT(bytes) as bytes_count,
                       AVG(response_time_ms) as response_time_ms_mean,
                       AVG(risk_score) as risk_score_mean
                FROM {source}
                GROUP BY hour, event_type
                ORDER BY hour, event_type
            """).fetchdf()
        except Exception:
            return conn.execute(f"""
                SELECT status_code, event_type, COUNT(*) as count
                FROM {source}
                GROUP BY status_code, event_type
            """).fetchdf()

# --- FireDucks Operations ---
def run_fireducks_operation(func: Callable[[PandasDataFrame], Any], csv_path: str) -> Any:
    """Run operation using FireDucks DataFrame"""
    try:
        import fireducks.pandas as fpd
        df = cast(PandasDataFrame, fpd.read_csv(csv_path))
        return func(df)
    except ImportError:
        raise ImportError("FireDucks is required but not available")
def fireducks_filter_group(csv_path: str) -> Optional[PandasDataFrame]:
    """
    FireDucks version of filter_group operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        Optional[PandasDataFrame]: Resulting DataFrame or None if FireDucks not available.
    """
    def _fireducks_filter_group(df: PandasDataFrame) -> PandasDataFrame:
        filtered = cast(PandasDataFrame, df[df["status_code"] == 200])
        return cast(PandasDataFrame, filtered.groupby("source_ip").agg({"bytes": "sum"}))
    try:
        return cast(PandasDataFrame, run_fireducks_operation(_fireducks_filter_group, csv_path))
    except ImportError:
        return None

def fireducks_stats(csv_path: str) -> Optional[PandasDataFrame]:
    """
    FireDucks version of stats operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        Optional[PandasDataFrame]: Resulting DataFrame or None if FireDucks not available.
    """
    def _fireducks_stats(df: PandasDataFrame) -> PandasDataFrame:
        return cast(PandasDataFrame, df.groupby("event_type").agg({
            "bytes": ["mean", "std", "min", "max"],
            "response_time_ms": ["mean", "median"],
            "risk_score": ["mean", "std"]
        }))
    try:
        return cast(PandasDataFrame, run_fireducks_operation(_fireducks_stats, csv_path))
    except ImportError:
        return None

def fireducks_complex(csv_path: str) -> Optional[PandasDataFrame]:
    """
    FireDucks version of complex operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        Optional[PandasDataFrame]: Top 10 by bytes per event_type or None if FireDucks not available.
    """
    def _fireducks_complex(df: PandasDataFrame) -> PandasDataFrame:
        grouped = cast(PandasDataFrame, df.groupby("source_ip").agg({
            "bytes": "sum", 
            "response_time_ms": "mean", 
            "risk_score": "mean"
        }).reset_index())
        grouped.columns = pd.Index(["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"])
        result = cast(PandasDataFrame, df.merge(grouped, on="source_ip"))
        result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
        return cast(PandasDataFrame, result[result["bytes_rank"] <= 10])
    try:
        return cast(PandasDataFrame, run_fireducks_operation(_fireducks_complex, csv_path))
    except ImportError:
        return None

def fireducks_timeseries(csv_path: str) -> Optional[PandasDataFrame]:
    """
    FireDucks version of timeseries operation.
    Args:
        csv_path (str): Path to CSV file.
    Returns:
        Optional[PandasDataFrame]: Resulting DataFrame or None if FireDucks not available.
    """
    def _fireducks_timeseries(df: PandasDataFrame) -> PandasDataFrame:
        if 'timestamp' in df.columns:
            df_copy = cast(PandasDataFrame, df.copy())
            df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
            df_copy['hour'] = df_copy['timestamp'].dt.hour
            result = cast(PandasDataFrame, df_copy.groupby(['hour', 'event_type']).agg({
                'bytes': ['sum', 'count'],
                'response_time_ms': 'mean',
                'risk_score': 'mean'
            }))
        else:
            counts = df.groupby(['status_code', 'event_type']).size()
            result = cast(PandasDataFrame, pd.DataFrame({'count': counts}).reset_index())
        return result
    try:
        return cast(PandasDataFrame, run_fireducks_operation(_fireducks_timeseries, csv_path))
    except ImportError:
        return None

# --- Benchmark Runner ---
def run_library_benchmarks(library_name: str, csv_path: str, repeat: int = 1) -> dict:
    """
    Run all operations for a single library, optionally repeating each operation.
    Args:
        library_name (str): Name of the library.
        csv_path (str): Path to CSV file.
        repeat (int): Number of times to repeat each operation.
    Returns:
        dict: Mapping of operation names to durations.
    """
    operations = {
        "filter_group": globals()[f"{library_name}_filter_group"],
        "statistics": globals()[f"{library_name}_stats"],
        "complex_join": globals()[f"{library_name}_complex"],
        "timeseries": globals()[f"{library_name}_timeseries"]
    }
    results = {}
    for operation_name, operation_func in operations.items():
        print(f"\n--- {library_name.upper()} {operation_name} ---")
        duration = run_benchmark_operation(library_name, operation_func, operation_name, csv_path)
        if duration is not None:
            # Clamp extremely small/zero durations to a tiny epsilon for visibility
            results[operation_name] = duration if duration > 0 else 0.000001
    return results

def run_all_benchmarks(csv_path: str, repeat: int = 1) -> dict:
    """
    Run all benchmarks for all available libraries, one library at a time.
    Args:
        csv_path (str): Path to CSV file.
        repeat (int): Number of times to repeat each operation.
    Returns:
        dict: Mapping of library names to their benchmark results.
    """
    libraries = ["pandas", "modin", "polars", "duckdb"]
    if FIREDUCKS_AVAILABLE:
        libraries.append("fireducks")
    results = {}
    for library_name in libraries:
        print(f"\n{'='*50}")
        print(f"Running benchmarks for {library_name.upper()}...")
        print(f"{'='*50}")
        library_results = run_library_benchmarks(library_name, csv_path)
        if library_results:
            results[library_name] = library_results
    return results

def save_results_to_csv(results: dict, host_info: dict, script_name: str, dataset_size: int, dataset_path: Union[str, Path]) -> None:
    """
    Save benchmark results to CSV file.
    Args:
        results (dict): Benchmark results.
        host_info (dict): Host system information.
        script_name (str): Name of the script creating the record.
        dataset_size (int): Number of records in the dataset.
    """
    # Explicit column order to match other scripts:
    # host info (18) -> dataset_size -> dataset_name -> dataset_format -> timings -> script_name
    host_order = [
        "timestamp", "hostname", "platform", "system", "release", "version", "machine", "processor",
        "cpu_count_logical", "cpu_count_physical", "cpu_freq_max", "cpu_freq_current",
        "memory_total_gb", "memory_available_gb", "python_version", "python_implementation",
        "cpu_brand", "cpu_arch"
    ]
    operations = ["filter_group", "statistics", "complex_join", "timeseries"]
    libraries = ["pandas", "modin", "polars", "duckdb", "fireducks"]

    # Build timing keys list in deterministic order
    timing_keys: List[str] = []
    timing_pairs: List[tuple[str, str]] = []  # (operation, library)
    for op in operations:
        for lib in libraries:
            timing_keys.append(f"{op}_{lib}_seconds")
            timing_pairs.append((op, lib))

    # Derive dataset metadata from provided path (not global default)
    try:
        ds_path = Path(dataset_path)
        ds_name = ds_path.name if ds_path.name else 'unknown'
        suffs = [s.lower() for s in ds_path.suffixes]
        comp = {'.gz', '.zip', '.zst', '.bz2'}
        base = [s for s in suffs if s not in comp]
        ext = (base[-1] if base else ds_path.suffix).lower().lstrip('.')
        if ext in ('jsonl', 'ndjson'):
            ext = 'ndjson'
        ds_fmt = ext or 'unknown'
    except Exception:
        ds_name = 'unknown'
        ds_fmt = 'unknown'

    # Construct row
    row = []
    for k in host_order:
        row.append(host_info.get(k))
    row.append(dataset_size)
    row.append(ds_name)
    row.append(ds_fmt)

    # Append timing values
    for (op_name, lib_name) in timing_pairs:
        val = results.get(lib_name, {}).get(op_name)
        if lib_name == "fireducks" and not FIREDUCKS_AVAILABLE:
            val = np.nan
        if val is None:
            val = np.nan
        row.append(val)

    row.append(script_name)

    # Prepare header (mirrors order)
    header = host_order + [
        "dataset_size", "dataset_name", "dataset_format"
    ] + timing_keys + ["script_name"]

    file_exists = os.path.exists(RESULTS_CSV_PATH)
    with open(RESULTS_CSV_PATH, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)
        print(f"Results saved to {RESULTS_CSV_PATH}")

def main():
    global client
    client = get_dask_client()
    parser = argparse.ArgumentParser(description="Comprehensive Data Processing Benchmark")
    parser.add_argument("-d", "--dataset", type=str, required=False, help="Path to input dataset file (auto-detect defaults if omitted)")
    parser.add_argument("-o", "--output", type=str, required=False, help="Path to results CSV file (default: data/benchmark_results.csv)")
    parser.add_argument("--repeat", type=int, default=1, help="Number of times to repeat each benchmark (default: 1)")
    args = parser.parse_args()

    csv_path = Path(args.dataset) if args.dataset else CSV_PATH
    results_path = Path(args.output) if args.output else RESULTS_CSV_PATH
    if not csv_path.exists():
        print(f"Warning: dataset path {csv_path} does not exist (will attempt to read anyway)")

    print("="*60)
    print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
    print("="*60)
    setup_modin()
    pd.set_option('display.float_format', '{:.0f}'.format)
    host_info = get_host_info()
    print(f"Running on: {host_info.get('hostname', 'Unknown')} ({host_info.get('platform', 'Unknown')})")
    print(f"CPU: {host_info.get('cpu_brand', 'Unknown')} ({host_info.get('cpu_count_logical', 'N/A')} logical cores)")
    print(f"Memory: {host_info.get('memory_total_gb', 'N/A')} GB total")
    print(f"\nStarting comprehensive benchmark with {csv_path}")
    print("This will test 4 different operations across all available libraries...")
    log_memory_usage("Initial memory usage")
    
    # Get dataset size
    dataset_size = get_dataset_size(str(csv_path))
    print(f"Dataset size: {dataset_size:,} records")
    
    results = run_all_benchmarks(str(csv_path), args.repeat)
    log_memory_usage("Final memory usage")
    script_name = "benchmark_01.py"  # Or use __file__.split('/')[-1]
    # Temporarily update global RESULTS_CSV_PATH for save function compatibility
    globals()['RESULTS_CSV_PATH'] = results_path
    save_results_to_csv(results, host_info, script_name, dataset_size, csv_path)
    
    # Cleanup Ray if it was initialized
    if DEFAULT_MODIN_ENGINE == "ray" and RAY_AVAILABLE:
        try:
            import ray
            if ray.is_initialized():
                ray.shutdown()
                print("Ray shutdown completed")
        except Exception as e:
            print(f"Warning: Ray shutdown failed: {e}")
    
    print(f"\n{'='*50}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*50}")
    for operation in ["filter_group", "statistics", "complex_join", "timeseries"]:
        print(f"\n{operation.upper()} Operation:")
        operation_timings = {}
        for library, timings in results.items():
            if operation in timings:
                operation_timings[library] = timings[operation]
        if operation_timings:
            fastest = min(operation_timings.items(), key=lambda x: x[1])
            print(f"  Fastest: {fastest[0]} ({fastest[1]:.2f}s)")
            for lib, duration in sorted(operation_timings.items(), key=lambda x: x[1]):
                speedup = fastest[1] / duration if duration > 0 else 0
                print(f"  {lib:10}: {duration:6.2f}s (x{speedup:.1f})")
    print(f"\nResults saved to: {results_path}")
    print("Benchmark completed!")
# --- End of benchmark.py ---

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()