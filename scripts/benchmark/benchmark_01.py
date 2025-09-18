"""
Comprehensive Data Processing Benchmark Script

Benchmarks multiple data processing libraries (pandas, modin, polars, duckdb, fireducks) on several operations.
Allows configuration of repeat count via command-line argument (default: 1).
Uses best software engineering practices, type hints, and docstrings.
"""

import time
from typing import Optional, Callable, Any, Union, List, TypeVar, cast
import argparse
import csv
import gc
import logging
import warnings
import sys
import os
from pathlib import Path
from datetime import datetime
import platform  # ensure present (near other imports)

import psutil
import numpy as np
import pandas as pd
import polars as pl
import duckdb

# Remove duplicate / conflicting imports
# from distributed import Client   # (Removed - we use dask.distributed.Client explicitly)
from dask.distributed import Client, LocalCluster

# Suppress noisy SyntaxWarnings
warnings.filterwarnings("ignore", category=SyntaxWarning, message=r"invalid escape sequence '\\_'")

# Project root path setup
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils import (
    get_host_info, get_memory_usage_mb, log_memory_usage,
    PlatformDetector, optimize_df_types
)
from utils.data_io import UniversalDataReader, get_dataset_size as universal_dataset_size

# Platform flags
platform_flags = PlatformDetector.get_platform_flags()
IS_WINDOWS = platform_flags['IS_WINDOWS']
IS_WSL = platform_flags['IS_WSL']
IS_LINUX = platform_flags['IS_LINUX']
IS_MACOS = platform_flags['IS_MACOS']

# Optional libraries
try:
    import modin.pandas as mpd
    MODIN_AVAILABLE = True
except ImportError:
    MODIN_AVAILABLE = False
    print("Warning: Modin not available, will be skipped.")

try:
    import fireducks.pandas as fpd
    FIREDUCKS_AVAILABLE = True
except ImportError:
    FIREDUCKS_AVAILABLE = False
    print("Warning: FireDucks not available, will be skipped.")

# Add missing import for RAY_AVAILABLE
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

# Type aliases
PandasDataFrame = pd.DataFrame
ModinDataFrame = mpd.DataFrame if MODIN_AVAILABLE else Any
PolarsDataFrame = pl.DataFrame
AnyDataFrame = Union[PandasDataFrame, PolarsDataFrame, ModinDataFrame]

DEFAULT_MODIN_ENGINE = "dask"

_universal_reader = UniversalDataReader(default_library='pandas')

def get_dataset_size(csv_path: str) -> int:
    """Return number of records for dataset path. Returns 0 on failure (caller handles)."""
    try:
        return universal_dataset_size(Path(csv_path))
    except Exception as e:
        print(f"Warning: Could not determine dataset size: {e}")
        return 0

def _detect_format(path: Union[str, Path]) -> str:
    """Best-effort format detection (csv, parquet, json, ndjson). Falls back to csv."""
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

def optimize_benchmark_df(bdf: pd.DataFrame) -> pd.DataFrame:
    """Apply custom dtype optimization rules to a pandas DataFrame for memory efficiency."""
    new_dict_types = {
        'datetime64[ns]': ['timestamp'],
        'category': ['source_ip', 'destination_ip', 'port', 'protocol', 'event_type',
                     'severity', 'user', 'status_code', 'country', 'device_type'],
        'uint32': ['bytes', 'session_id'],
        'uint16': ['response_time_ms'],
        'float32': ['risk_score']
    }
    return optimize_df_types(bdf, new_dict_types)

def _read_pandas(path: str) -> pd.DataFrame:
    """Read CSV/Parquet into pandas and optimize types."""
    if path.endswith('.parquet'):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    try:
        opt = optimize_benchmark_df(df)
        del df
        gc.collect()
        return opt
    except Exception as e:
        print(f"Warning: Optimization failed for {path}: {e}. Using original.")
        return df

def _read_modin(path: str) -> ModinDataFrame:
    """Read dataset into Modin DataFrame with conservative optimization (fallback-safe)."""
    if not MODIN_AVAILABLE:
        raise RuntimeError("Modin not installed.")
    fmt = _detect_format(path)
    if fmt == 'parquet':
        df = mpd.read_parquet(path)
    elif fmt == 'json':
        df = mpd.read_json(path)
    elif fmt == 'ndjson':
        df = mpd.read_json(path, lines=True)
    else:
        df = mpd.read_csv(path)
    try:
        # Light-touch optimization (avoid full convert to pandas for huge sets)
        #if 'risk_score' in df.columns:
         #   df['risk_score'] = df['risk_score'].astype('float32')
        #return df
        return optimize_benchmark_df(df)  # Full optimization
    except Exception as e:
        print(f"Warning: Modin optimization failed: {e}.")
        return df

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

def _read_polars(path: str) -> PolarsDataFrame:
    """
    Read dataset into Polars DataFrame (eager loading, format-aware).
    Args:
        path (str): Path to the file.
    Returns:
        PolarsDataFrame: Loaded DataFrame.
    """
    fmt = _detect_format(path)
    if fmt == 'csv':
        return cast(PolarsDataFrame, pl.read_csv(path))
    elif fmt == 'parquet':
        return cast(PolarsDataFrame, pl.read_parquet(path))
    elif fmt == 'json':
        return cast(PolarsDataFrame, pl.read_json(path))
    elif fmt == 'ndjson':
        try:
            return cast(PolarsDataFrame, pl.read_ndjson(path))
        except Exception:
            return cast(PolarsDataFrame, pl.read_json(path, lines=True))
    else:
        return cast(PolarsDataFrame, pl.read_csv(path))
    
def _duckdb_source(path: str) -> str:
    fmt = _detect_format(path)
    if fmt == 'parquet':
        return f"read_parquet('{path}')"
    if fmt in ('json', 'ndjson'):
        return f"read_json_auto('{path}')"
    return f"read_csv_auto('{path}')"

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
        if IS_WINDOWS:
            n_workers = min(2, max(1, logical))
            threads_per_worker = max(1, logical // n_workers)
            cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
                processes=False,
                silence_logs=logging.CRITICAL,
            )
            return Client(cluster)

        target_per_worker_gb = 4
        n_workers_by_mem = max(1, int(total_gb // target_per_worker_gb))
        n_workers = min(logical, n_workers_by_mem)
        n_workers = max(1, n_workers)
        threads_per_worker = max(1, logical // n_workers)

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

# Update setup_modin to accept csv_path
def setup_modin(csv_path: str) -> None:
    """Configure Modin+Dask cluster or disable if system/data constraints exceed thresholds."""
    try:
        total_memory_gb = psutil.virtual_memory().total / (1024**3)
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        logical_cores = psutil.cpu_count(logical=True) or 4
        dataset_size = get_dataset_size(csv_path)

        # Disable for large datasets on modest RAM
        if (not MODIN_AVAILABLE or
            total_memory_gb < 4 or
            available_memory_gb < 2 or
            logical_cores < 4 or
            dataset_size > 12_000_000):
            print("Warning: System specs or dataset size too large for Modin; disabling.")
            globals()['__modin_disabled'] = True
            return

        if total_memory_gb < 16:
            print(f"Warning: System has only {total_memory_gb:.1f}GB RAM. Modin may fail.")
        if available_memory_gb < 4:
            print(f"Warning: Only {available_memory_gb:.1f}GB free. Reducing workers.")

        import modin.config as cfg
        cfg.Engine.put("dask")
        cfg.StorageFormat.put("pandas")

        logical_cores = psutil.cpu_count(logical=True) or 4
        max_workers = min(logical_cores, max(1, int(available_memory_gb / 5)))
        n_workers = max(1, max_workers)
        memory_per_worker_gb = max(0.5, available_memory_gb * 0.6 / n_workers)

        from dask import config as dask_config
        dask_config.set({
            "distributed.worker.memory.target": 0.8,
            "distributed.worker.memory.spill": 0.9,
            "distributed.worker.memory.pause": 0.95,
            "distributed.worker.memory.terminate": 0.98,
            "dataframe.shuffle.method": "tasks"
        })

        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=1,
            processes=not IS_WINDOWS,
            memory_limit=f"{memory_per_worker_gb:.2f}GB",
            silence_logs=logging.CRITICAL,
        )
        client = Client(cluster)
        globals()['client'] = client
        print(f"Dask cluster configured: {n_workers} workers, {memory_per_worker_gb:.2f}GB per worker")

        globals()['__modin_failure_count'] = 0
        globals()['__modin_failure_threshold'] = 1
        globals()['__modin_disabled'] = False
        print("Modin enabled with optimization support.")
    except Exception as e:
        print(f"Warning: Modin setup failed: {e}")
        globals()['__modin_disabled'] = True

def run_modin_operation(func: Callable[[ModinDataFrame], Any], csv_path: str) -> Any:
    """Execute a Modin operation with retry + fallback, tracking failures and disabling if needed."""
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

        # Diagnostics
        try:
            if client is not None:
                info = client.scheduler_info()
                workers = info.get('workers', {})
                print(f"Dask scheduler workers: {len(workers)}")
                for addr, winfo in workers.items():
                    mem_limit = winfo.get('memory_limit')
                    mem = winfo.get('memory') or winfo.get('metrics', {}).get('memory')
                    print(f" - worker {addr}: memory_limit={mem_limit}, memory_used={mem}")
        except Exception:
            pass

        failure_count += 1
        globals()['__modin_failure_count'] = failure_count
        if failure_count >= failure_threshold:
            globals()['__modin_disabled'] = True
            print(f"Modin disabled after {failure_count} failures.")
            return None

        # Fallback to single-worker cluster
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
            failure_count = globals().get('__modin_failure_count', 0) + 1
            globals()['__modin_failure_count'] = failure_count
            if failure_count >= failure_threshold:
                globals()['__modin_disabled'] = True
            return None
    finally:
        gc.collect()

def run_duckdb_operation(func: Callable[[str, duckdb.DuckDBPyConnection], Any], csv_path: str) -> Any:
    """Run an operation with a fresh DuckDB connection, ensuring isolation and cleanup."""
    conn = duckdb.connect()
    try:
        return func(csv_path, conn)
    except Exception as e:
        print(f"DuckDB operation failed: {e}")
        return None
    finally:
        conn.close()
        gc.collect()

def duckdb_timeseries(csv_path: str):
    """Time-series grouping in DuckDB; tries timestamp first, falls back to status_code."""
    def operation(path, conn):
        # Attempt timestamp-based grouping
        try:
            query_ts = f"""
            SELECT 
                date_part('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
                event_type,
                SUM(bytes) AS bytes_sum,
                COUNT(*) AS bytes_count,
                AVG(response_time_ms) AS response_time_ms_mean,
                AVG(risk_score) AS risk_score_mean
            FROM '{path}'
            GROUP BY hour, event_type
            """
            return conn.execute(query_ts).df()
        except Exception:
            query_fallback = f"""
            SELECT 
                status_code AS hour,
                event_type,
                SUM(bytes) AS bytes_sum,
                COUNT(*) AS bytes_count,
                AVG(response_time_ms) AS response_time_ms_mean,
                AVG(risk_score) AS risk_score_mean
            FROM '{path}'
            GROUP BY hour, event_type
            """
            return conn.execute(query_fallback).df()
    return run_duckdb_operation(operation, csv_path)

def duckdb_filter_group(csv_path: str) -> pd.DataFrame:
    """
    DuckDB version of filter_group operation using SQL.
    Filters for status_code=200, groups by source_ip, sums bytes.
    """
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        return conn.execute(f"""
            SELECT source_ip, SUM(bytes) as bytes
            FROM {source}
            WHERE status_code = 200
            GROUP BY source_ip
        """).fetchdf()
    return run_duckdb_operation(operation, csv_path)

def duckdb_statistics(csv_path: str) -> pd.DataFrame:
    """
    DuckDB version of statistics operation using SQL.
    Computes aggregates per event_type.
    """
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
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
    return run_duckdb_operation(operation, csv_path)

def duckdb_complex_join(csv_path: str) -> pd.DataFrame:
    """
    DuckDB version of complex_join operation using SQL window functions.
    Performs ranking and joins for top 10 per event_type.
    """
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
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
    return run_duckdb_operation(operation, csv_path)

def run_polars_operation(func: Callable[[pl.DataFrame], Any], csv_path: str) -> Any:
    """
    Execute a Polars operation with error handling and memory logging.
    Args:
        func: Function that takes a Polars DataFrame and returns a result.
        csv_path: Path to the CSV file.
    Returns:
        Result of the operation or None on failure.
    """
    try:
        log_memory_usage(f"[polars {func.__name__} (start)]")
        df = _read_polars(csv_path)
        result = func(df)
        log_memory_usage(f"[polars {func.__name__} (end)]")
        return result
    except Exception as e:
        print(f"Polars operation {func.__name__} failed: {e}")
        log_memory_usage(f"[polars {func.__name__} (end)]")
        return None

def modin_filter_group(csv_path: str) -> Optional[ModinDataFrame]:
    """Modin wrapper for filter_group: delegates to pandas function after optimization."""
    return run_modin_operation(pandas_filter_group, csv_path)

def modin_statistics(csv_path: str) -> Optional[ModinDataFrame]:
    """Modin wrapper for statistics: delegates to pandas function after optimization."""
    return run_modin_operation(pandas_statistics, csv_path)

def modin_complex_join(csv_path: str) -> Optional[ModinDataFrame]:
    """Modin wrapper for complex_join: delegates to pandas function after optimization."""
    return run_modin_operation(pandas_complex_join, csv_path)

def modin_timeseries(csv_path: str) -> Optional[ModinDataFrame]:
    """Modin wrapper for timeseries: delegates to pandas function after optimization."""
    return run_modin_operation(pandas_timeseries, csv_path)

def run_pandas_operation(func: Callable[[PandasDataFrame], Any], csv_path: str) -> Any:
    """
    Load the dataset with pandas (CSV or Parquet), run the provided function,
    and return the result. Returns None on failure.
    """
    try:
        df = _read_pandas(csv_path)
        result = func(df)
        del df
        gc.collect()
        return result
    except Exception as e:
        print(f"Pandas operation failed: {e}")
        gc.collect()
        return None

def pandas_filter_group(csv_path: str):
    """
    Filter rows where status_code == 200 and aggregate sum(bytes) by source_ip.
    Returns None if required columns are missing.
    """
    required = {"status_code", "source_ip", "bytes"}
    def op(df: pd.DataFrame):
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            print(f"Pandas filter_group skipped (missing columns: {missing})")
            return None
        return (
            df[df["status_code"] == 200]
            .groupby("source_ip", observed=False)
            .agg({"bytes": "sum"})
        )
    return run_pandas_operation(op, csv_path)

def pandas_statistics(csv_path: str):
    return run_pandas_operation(lambda df: df.groupby("event_type", observed=False).agg({
        "bytes": ["mean", "std", "min", "max"],
        "response_time_ms": ["mean", "median"],
        "risk_score": ["mean", "std"]
    }), csv_path)

def pandas_complex_join(csv_path: str):
    """
    Build per-source_ip summary, join back, rank bytes per event_type, keep top 10.
    """
    needed = {"source_ip", "bytes", "response_time_ms", "risk_score", "event_type"}
    def op(df: pd.DataFrame):
        if not needed.issubset(df.columns):
            print(f"Pandas complex_join skipped (missing: {needed - set(df.columns)})")
            return None
        summary = (
            df.groupby("source_ip", observed=False)
              .agg({
                  "bytes": "sum",
                  "response_time_ms": "mean",
                  "risk_score": "mean"
              })
              .reset_index()
              .rename(columns={
                  "bytes": "total_bytes",
                  "response_time_ms": "avg_response_time_ms",
                  "risk_score": "avg_risk_score"
              })
        )
        merged = df.merge(summary, on="source_ip", how="left")
        merged["bytes_rank"] = (
            merged.groupby("event_type", observed=False)["bytes"]
                  .rank(method="dense", ascending=False)
        )
        return merged[merged["bytes_rank"] <= 10]
    return run_pandas_operation(op, csv_path)

def pandas_timeseries(csv_path: str):
    """
    Group by hour (derived from timestamp if present else status_code) and event_type,
    aggregating bytes, count, response_time_ms mean, and risk_score mean.
    """
    def op(df: pd.DataFrame):
        if "event_type" not in df.columns or "bytes" not in df.columns:
            print("Pandas timeseries skipped (missing event_type/bytes).")
            return None
        work = df.copy()
        if "timestamp" in work.columns:
            try:
                work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
                work["hour"] = work["timestamp"].dt.hour
            except Exception:
                work["hour"] = 0
        else:
            # Fallback: use status_code if present; else constant
            work["hour"] = work["status_code"] if "status_code" in work.columns else 0
        agg_spec = {"bytes": ["sum", "count"]}
        if "response_time_ms" in work.columns:
            agg_spec["response_time_ms"] = ["mean"]
        if "risk_score" in work.columns:
            agg_spec["risk_score"] = ["mean"]
        return work.groupby(["hour", "event_type"], observed=False).agg(agg_spec)
    return run_pandas_operation(op, csv_path)

def polars_filter_group(csv_path: str):
    return run_polars_operation(lambda df: df.filter(pl.col("status_code") == 200).group_by("source_ip").agg(pl.col("bytes").sum()), csv_path)

def polars_statistics(csv_path: str):
    return run_polars_operation(lambda df: df.group_by("event_type").agg([
        pl.col("bytes").mean().alias("bytes_mean"),
        pl.col("bytes").std().alias("bytes_std"),
        pl.col("bytes").min().alias("bytes_min"),
        pl.col("bytes").max().alias("bytes_max"),
        pl.col("response_time_ms").mean().alias("response_time_ms_mean"),
        pl.col("response_time_ms").median().alias("response_time_ms_median"),
        pl.col("risk_score").mean().alias("risk_score_mean"),
        pl.col("risk_score").std().alias("risk_score_std")
    ]), csv_path)

def polars_complex_join(csv_path: str):
    def complex_operation(df):
        summary = df.group_by("source_ip").agg([
            pl.col("bytes").sum().alias("total_bytes"),
            pl.col("response_time_ms").mean().alias("avg_response_time_ms"),
            pl.col("risk_score").mean().alias("avg_risk_score")
        ])
        result = df.join(summary, on="source_ip", how="left").with_columns(
            pl.col("bytes").rank("dense", descending=True).over("event_type").alias("bytes_rank")
        )
        return result.filter(pl.col("bytes_rank") <= 10)
    return run_polars_operation(complex_operation, csv_path)

def polars_timeseries(csv_path: str):
    def timeseries_operation(df):
        if "timestamp" in df.columns:
            return (df
                .with_columns(pl.col("timestamp").str.to_datetime().alias("timestamp"))
                .with_columns(pl.col("timestamp").dt.hour().alias("hour"))
                .group_by(["hour", "event_type"])
                .agg([
                    pl.col("bytes").sum().alias("bytes_sum"),
                    pl.col("bytes").count().alias("bytes_count"),
                    pl.col("response_time_ms").mean().alias("response_time_ms_mean"),
                    pl.col("risk_score").mean().alias("risk_score_mean")
                ])
            )
        else:
            return (df
                .with_columns(pl.col("status_code").alias("hour"))
                .group_by(["hour", "event_type"])
                .agg([
                    pl.col("bytes").sum().alias("bytes_sum"),
                    pl.col("bytes").count().alias("bytes_count"),
                    pl.col("response_time_ms").mean().alias("response_time_ms_mean"),
                    pl.col("risk_score").mean().alias("risk_score_mean")
                ])
            )
    return run_polars_operation(timeseries_operation, csv_path)

def run_benchmark_operation(
    library_name: str,
    operation_func: Callable,
    operation_name: str,
    csv_path: str
) -> Optional[float]:
    """Time a single (library, operation) pair; returns duration or None on failure."""
    log_memory_usage(f"{library_name} {operation_name} (start)")
    start = time.perf_counter()
    result = operation_func(csv_path)
    duration = time.perf_counter() - start
    log_memory_usage(f"{library_name} {operation_name} (end)")
    print(f"{library_name} {operation_name} duration: {duration:.2f}s")
    gc.collect()
    if result is None:
        return None
    return max(duration, 1e-6)

def run_library_benchmarks(library_name: str, csv_path: str, repeat: int = 1) -> dict:
    """Execute all benchmark operations for a single library; returns mapping op->duration."""
    operations = {}
    for op in ["filter_group", "statistics", "complex_join", "timeseries"]:
        fn = f"{library_name}_{op}"
        if fn in globals():
            operations[op] = globals()[fn]
        else:
            print(f"ERROR: Missing function {fn}; skipping {library_name}.")
            return {}
    out = {}
    for op_name, op_func in operations.items():
        print(f"\n--- {library_name.upper()} {op_name} ---")
        durations: List[float] = []
        for _ in range(repeat):
            dur = run_benchmark_operation(library_name, op_func, op_name, csv_path)
            if dur is not None:
                durations.append(dur)
        if not durations:
            out[op_name] = np.nan
        else:
            out[op_name] = float(np.mean(durations))
    return out

def run_all_benchmarks(csv_path: str, repeat: int = 1) -> dict:
    """Run benchmarks for all libraries (including modin even if later skipped)."""
    libraries = ["pandas", "modin", "polars", "duckdb"]
    if FIREDUCKS_AVAILABLE:
        libraries.append("fireducks")
    results = {}
    for lib in libraries:
        print(f"\n{'='*50}\nRunning benchmarks for {lib.upper()}...\n{'='*50}")
        results[lib] = run_library_benchmarks(lib, csv_path, repeat)
    return results

def cleanup_modin():
    """Close Dask client (if any) and free related resources."""
    try:
        client = globals().get('client')
        if client:
            try:
                client.close()
                print("Custom Dask client closed")
            except Exception:
                pass
            globals()['client'] = None
    finally:
        gc.collect()

EXPECTED_HOST_FIELDS = [
    "hostname","os_version","platform","os_major","os_minor","os_build",
    "architecture","processor","logical_cores","physical_cores",
    "max_frequency_mhz","current_frequency_mhz",
    "total_memory_gb","available_memory_gb",
    "python_version","python_implementation","cpu_brand","cpu_arch"
]

def normalize_host_info(raw: dict) -> dict:
    """Normalize host_info dict to expected keys with safe fallbacks."""
    # Map common alternative keys if original function returns different names
    alt_map = {
        "os_version": ["os_version","platform_version","version","release"],
        "platform": ["platform","system"],
        "architecture": ["architecture","machine"],
        "processor": ["processor","cpu"],
        "cpu_brand": ["cpu_brand","brand_raw","brand"],
        "cpu_arch": ["cpu_arch","architecture","machine"]
    }
    norm = {}
    for k in EXPECTED_HOST_FIELDS:
        if k in raw:
            norm[k] = raw[k]
            continue
        # try alternates
        if k in alt_map:
            val = None
            for alt in alt_map[k]:
                if alt in raw:
                    val = raw[alt]
                    break
            norm[k] = val if val is not None else ""
        else:
            norm[k] = raw.get(k, "")

    # Type coercions / defaults
    for numeric in ["logical_cores","physical_cores","max_frequency_mhz","current_frequency_mhz"]:
        try:
            norm[numeric] = int(norm.get(numeric) or 0)
        except Exception:
            norm[numeric] = 0
    for mem in ["total_memory_gb","available_memory_gb"]:
        try:
            norm[mem] = float(norm.get(mem) or 0.0)
        except Exception:
            norm[mem] = 0.0
    return norm

def _collect_system_fallbacks() -> dict:
    """Gather system metrics with robust fallbacks (used if host_info is partial)."""
    try:
        freq = psutil.cpu_freq()
    except Exception:
        freq = None
    vm = psutil.virtual_memory()
    return {
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "processor": platform.processor() or "",
        "cpu_count_logical": psutil.cpu_count(logical=True) or 0,
        "cpu_count_physical": psutil.cpu_count(logical=False) or 0,
        "cpu_freq_max": float(getattr(freq, "max", 0.0) or 0.0),
        "cpu_freq_current": float(getattr(freq, "current", 0.0) or 0.0),
        "memory_total_gb": vm.total / (1024**3),
        "memory_available_gb": vm.available / (1024**3),
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation()
    }

def _normalize_host_info_legacy(host_info: dict) -> dict:
    """
    Map internal host_info (possibly using os_version / architecture naming) to legacy CSV schema.
    Missing values are filled via fallbacks.
    """
    fallbacks = _collect_system_fallbacks()
    # Accept multiple possible source keys
    def pick(keys, default=""):
        for k in keys:
            if k in host_info and host_info[k] not in (None, "", 0):
                return host_info[k]
        return fallbacks.get(keys[0], default)

    out = {
        "hostname": pick(["hostname","host","node"]),
        "platform": pick(["platform","platform_full","os_version"]),
        "system": pick(["system","os","os_name"]),
        "release": pick(["release","os_release"]),
        "version": pick(["version","os_version","platform_version"]),
        "machine": pick(["machine","architecture","cpu_arch"]),
        "processor": pick(["processor","cpu_brand","cpu"]),
        "cpu_count_logical": pick(["logical_cores","cpu_count_logical"]),
        "cpu_count_physical": pick(["physical_cores","cpu_count_physical"]),
        "cpu_freq_max": pick(["max_frequency_mhz","cpu_freq_max"], 0),
        "cpu_freq_current": pick(["current_frequency_mhz","cpu_freq_current"], 0),
        "memory_total_gb": pick(["total_memory_gb","memory_total_gb"], 0.0),
        "memory_available_gb": pick(["available_memory_gb","memory_available_gb"], 0.0),
        "python_version": pick(["python_version"]),
        "python_implementation": pick(["python_implementation"]),
        "cpu_brand": pick(["cpu_brand","processor"]),
        "cpu_arch": pick(["cpu_arch","machine","architecture"])
    }

    # Numeric coercions & fallback fill if still zero
    fb = _collect_system_fallbacks()
    for k in ("cpu_count_logical","cpu_count_physical"):
        try:
            out[k] = int(out[k]) if int(out[k]) > 0 else fb[k]
        except Exception:
            out[k] = fb[k]
    for k in ("cpu_freq_max","cpu_freq_current","memory_total_gb","memory_available_gb"):
        try:
            val = float(out[k])
            if val <= 0:
                out[k] = fb[k]
            else:
                out[k] = val
        except Exception:
            out[k] = fb[k]
    return out

LEGACY_HEADER = [
    "timestamp","hostname","platform","system","release","version","machine","processor",
    "cpu_count_logical","cpu_count_physical","cpu_freq_max","cpu_freq_current",
    "memory_total_gb","memory_available_gb","python_version","python_implementation",
    "cpu_brand","cpu_arch","dataset_size","dataset_name","dataset_format",
    "filter_group_pandas_seconds","filter_group_modin_seconds","filter_group_polars_seconds",
    "filter_group_duckdb_seconds","filter_group_fireducks_seconds",
    "statistics_pandas_seconds","statistics_modin_seconds","statistics_polars_seconds",
    "statistics_duckdb_seconds","statistics_fireducks_seconds",
    "complex_join_pandas_seconds","complex_join_modin_seconds","complex_join_polars_seconds",
    "complex_join_duckdb_seconds","complex_join_fireducks_seconds",
    "timeseries_pandas_seconds","timeseries_modin_seconds","timeseries_polars_seconds",
    "timeseries_duckdb_seconds","timeseries_fireducks_seconds",
    "script_name"
]

def save_results_to_csv(results: dict, host_info: dict, script_name: str, dataset_size: int, output_path: Union[str, Path]) -> None:
    """Save benchmark results to CSV file with explicit column order."""
    host_order = [
        "timestamp", "hostname", "platform", "system", "release", "version", "machine", "processor",
        "cpu_count_logical", "cpu_count_physical", "cpu_freq_max", "cpu_freq_current",
        "memory_total_gb", "memory_available_gb", "python_version", "python_implementation",
        "cpu_brand", "cpu_arch"
    ]
    operations = ["filter_group", "statistics", "complex_join", "timeseries"]
    libraries = ["pandas", "modin", "polars", "duckdb", "fireducks"]

    timing_keys = [f"{op}_{lib}_seconds" for op in operations for lib in libraries]
    timing_pairs = [(op, lib) for op in operations for lib in libraries]

    # Use output_path to determine dataset info
    try:
        ds_path = Path(output_path).parent / "raw" / "synthetic_logs_10M.csv"  # Infer from structure
        if not ds_path.exists():
            ds_path = Path("synthetic_logs_10M.csv")  # Fallback
        ds_name = ds_path.name or 'unknown'
        ds_fmt = 'csv'  # Most common case
    except Exception:
        ds_name = 'unknown'
        ds_fmt = 'unknown'

    legacy_host = _normalize_host_info_legacy(host_info)
    timestamp = datetime.now().isoformat()

    row = [timestamp] + [legacy_host.get(k) for k in host_order[1:]] + [dataset_size, ds_name, ds_fmt]
    for op_name, lib_name in timing_pairs:
        val = results.get(lib_name, {}).get(op_name)
        if lib_name == "fireducks" and not FIREDUCKS_AVAILABLE:
            val = np.nan
        row.append(val if val is not None else np.nan)
    row.append(script_name)

    header = host_order + ["dataset_size", "dataset_name", "dataset_format"] + timing_keys + ["script_name"]
    
    # Use output_path instead of global RESULTS_CSV_PATH
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    
    file_exists = out_file.exists()
    with open(out_file, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)
    #print(f"Results saved to {out_file}")

def print_summary(results: dict) -> None:
    """Print a comparative summary of benchmark timings with relative speed factors."""
    print("\n" + "="*50)
    print("BENCHMARK SUMMARY")
    print("="*50 + "\n")
    operations = ["filter_group", "statistics", "complex_join", "timeseries"]
    libraries = ["pandas", "modin", "polars", "duckdb"]
    for op in operations:
        print(f"{op.upper().replace('_',' ')} Operation:")
        timings = {}
        for lib in libraries:
            val = results.get(lib, {}).get(op)
            if val is not None and not (isinstance(val, float) and np.isnan(val)) and val > 0:
                timings[lib] = val
        if not timings:
            print("  No valid timings.\n")
            continue
        fastest_lib = min(timings, key=lambda k: timings[k])
        fastest_time = timings[fastest_lib]
        print(f"  Fastest: {fastest_lib} ({fastest_time:.2f}s)")
        for lib, t in sorted(timings.items(), key=lambda x: x[1]):
            factor = fastest_time / t if t > 0 else 0
            print(f"  {lib:<10}: {t:7.2f}s (x{factor:.1f})")
        print()

def main():
    """CLI entrypoint for running the full benchmark workflow."""
    global client
    client = get_dask_client()
    parser = argparse.ArgumentParser(description="Comprehensive Data Processing Benchmark")
    parser.add_argument("-d","--dataset", type=str, help="Path to dataset file")
    parser.add_argument("-o","--output", type=str, help="Output CSV file path")
    parser.add_argument("-r","--repeat", type=int, default=1, help="Repeat count per operation")
    args = parser.parse_args()

    dataset_path = Path(args.dataset) if args.dataset else Path("data/raw/synthetic_logs_test.csv")
    output_path = Path(args.output) if args.output else Path("data/benchmark_results.csv")

    try:  # Wrap main logic in try
        print("="*60)
        print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
        print("="*60)
        setup_modin(str(dataset_path))
        pd.set_option('display.float_format', '{:.0f}'.format)

        raw_host_info = get_host_info()
        host_info = normalize_host_info(raw_host_info)

        # Fallbacks if provider omitted or returned zeros
        if host_info.get("logical_cores", 0) <= 0:
            host_info["logical_cores"] = psutil.cpu_count(logical=True) or 0
        if host_info.get("physical_cores", 0) <= 0:
            host_info["physical_cores"] = psutil.cpu_count(logical=False) or 0
        if host_info.get("total_memory_gb", 0) <= 0:
            host_info["total_memory_gb"] = psutil.virtual_memory().total / (1024**3)
        if host_info.get("available_memory_gb", 0) <= 0:
            host_info["available_memory_gb"] = psutil.virtual_memory().available / (1024**3)

        print(f"Running on: {host_info.get('hostname','?')} ({host_info.get('os_version','unknown')})")
        print(f"CPU: {host_info.get('cpu_brand','?')} ({host_info.get('logical_cores',0)} logical cores)")
        print(f"Memory: {host_info.get('total_memory_gb',0.0):.2f} GB total\n")

        if not dataset_path.exists():
            print(f"ERROR: Dataset file not found: {dataset_path}")
            return

        log_memory_usage("Initial memory usage")

        dataset_size = get_dataset_size(str(dataset_path))
        if dataset_size == 0:
            print("ERROR: Dataset appears empty or unreadable.")
            return
        print(f"Dataset size: {dataset_size:,} records")

        avail_gb = psutil.virtual_memory().available / (1024**3)
        if avail_gb < 4:
            print(f"Warning: Only {avail_gb:.1f}GB available; operations may fail.")

        print("Beginning benchmarks...")
        results = run_all_benchmarks(str(dataset_path), args.repeat)

        log_memory_usage("Final memory usage")
        # Fix: Use correct number of arguments (5 instead of 6)
        save_results_to_csv(results, raw_host_info, "benchmark_01.py", dataset_size, str(output_path))
        print(f"Results saved to {output_path}")

        cleanup_modin()
        print_summary(results)
        print("Benchmark completed!")
    finally:  # Proper finally block
        # Add Ray cleanup
        if DEFAULT_MODIN_ENGINE == "ray" and RAY_AVAILABLE:
            try:
                import ray
                if ray.is_initialized():
                    ray.shutdown()
                    print("Ray shutdown completed")
            except Exception as e:
                print(f"Warning: Ray shutdown failed: {e}")

if __name__ == "__main__":
    main()