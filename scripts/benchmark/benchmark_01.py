"""
Comprehensive Data Processing Benchmark Script

Benchmarks multiple data processing libraries (pandas, polars, duckdb, fireducks) on several operations.
Allows configuration of repeat count via command-line argument (default: 1).
Uses best software engineering practices, type hints, and docstrings.
"""

import time
from typing import Optional, Callable, Any, Union, List, cast
import argparse
import csv
import gc
import warnings
import sys
import os
import math  # FIXED: Moved import to top for consistency
from pathlib import Path
from datetime import datetime
import platform

import psutil
import numpy as np
import pandas as pd
import polars as pl
import duckdb

# Suppress noisy SyntaxWarnings
warnings.filterwarnings("ignore", category=SyntaxWarning, message=r"invalid escape sequence '\\_'")

# Project root path setup
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils import ( get_host_info, log_memory_usage, optimize_df_types )
from utils.data_io import UniversalDataReader, get_dataset_size as universal_dataset_size
from utils.platform_utils import FIREDUCKS_AVAILABLE  

if FIREDUCKS_AVAILABLE:
    import fireducks.pandas as fpd
else:
    fpd = None

# Type aliases
PandasDataFrame = pd.DataFrame
PolarsDataFrame = pl.DataFrame
AnyDataFrame = Union[PandasDataFrame, PolarsDataFrame]

_universal_reader = UniversalDataReader(default_library='pandas')

def get_dataset_size(csv_path: str) -> int:
    """
    Return the number of records in the dataset file.

    Args:
        csv_path (str): Path to the dataset file.

    Returns:
        int: Number of records, or 0 on failure.
    """
    try:
        return universal_dataset_size(Path(csv_path))
    except Exception as e:
        print(f"Warning: Could not determine dataset size: {e}")
        return 0

def _detect_format(path: Union[str, Path]) -> str:
    """
    Detect the file format (csv, parquet, json, ndjson) with fallbacks.

    Args:
        path (Union[str, Path]): Path to the file.

    Returns:
        str: Detected format ('csv' as default).
    """
    try:
        return _universal_reader.detect_file_format(Path(path))
    except Exception:
        p = Path(path)
        suffs = [s for s in p.suffixes if s.lower() not in {'.gz', '.zip', '.zst', '.bz2'}]
        if suffs:
            ext = suffs[-1].lower()
            if ext in ('.jsonl', '.ndjson'):
                return 'ndjson'
            if ext == '.json':
                return 'json'
            if ext == '.parquet':
                return 'parquet'
        return 'csv'

def optimize_benchmark_df(bdf: pd.DataFrame) -> pd.DataFrame:
    """
    Apply custom dtype optimization rules to a pandas DataFrame for memory efficiency.
    Matches the original benchmark.py optimizations (port as uint16, not category).

    Args:
        bdf (pd.DataFrame): Input DataFrame to optimize.

    Returns:
        pd.DataFrame: Optimized DataFrame.
    """
    new_dict_types = {
        'datetime64[ns]': ['timestamp'],
        'category': ['source_ip', 'destination_ip', 'protocol', 'event_type',
                     'severity', 'user', 'status_code', 'country', 'device_type'],
        'uint32': ['bytes', 'session_id'],
        'uint16': ['response_time_ms', 'port'],  # FIXED: port as uint16, not category
        'float32': ['risk_score']
    }
    return optimize_df_types(bdf, new_dict_types)

def load_and_optimize_pandas(csv_path: str) -> pd.DataFrame:
    """
    Load and optimize a pandas DataFrame once per library benchmark.

    Args:
        csv_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Optimized DataFrame.

    Raises:
        Exception: If loading or optimization fails.
    """
    if csv_path.endswith('.parquet'):
        df = pd.read_parquet(csv_path)
    else:
        df = pd.read_csv(csv_path)
    
    try:
        original_memory = df.memory_usage(deep=True).sum()
        opt = optimize_benchmark_df(df)
        optimized_memory = opt.memory_usage(deep=True).sum()
        memory_reduction = (original_memory - optimized_memory) / original_memory * 100
        print(f"  pandas DataFrame optimized: {memory_reduction:.1f}% memory reduction ({original_memory/1024/1024:.1f}MB → {optimized_memory/1024/1024:.1f}MB)")
        del df
        gc.collect()
        return opt
    except Exception as e:
        print(f"Warning: Optimization failed: {e}")
        return df

def _read_polars(path: str) -> PolarsDataFrame:
    """
    Read dataset into a Polars DataFrame with format detection.

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
    """
    Generate DuckDB source string based on file format.

    Args:
        path (str): Path to the file.

    Returns:
        str: DuckDB source query string.
    """
    fmt = _detect_format(path)
    if fmt == 'parquet':
        return f"read_parquet('{path}')"
    if fmt in ('json', 'ndjson'):
        return f"read_json_auto('{path}')"
    return f"read_csv_auto('{path}')"

def run_duckdb_operation(func: Callable[[str, duckdb.DuckDBPyConnection], Any], csv_path: str) -> Any:
    """
    Run a DuckDB operation with a fresh connection for isolation.

    Args:
        func (Callable): Function to execute with path and connection.
        csv_path (str): Path to the dataset.

    Returns:
        Any: Result of the operation, or None on failure.
    """
    conn = duckdb.connect()
    try:
        return func(csv_path, conn)
    except Exception as e:
        print(f"DuckDB operation failed: {e}")
        return None
    finally:
        conn.close()
        gc.collect()

# -------- Canonical Operation Implementations (shared semantics) --------
# Operations mirror original benchmark.py: filter bytes > 1000, group by event_type, count rows, etc.

# ----------------- Pandas -----------------
def run_pandas_operation(func: Callable[[PandasDataFrame], Any],
                         csv_path: str,
                         cached_df: Optional[pd.DataFrame] = None) -> Any:
    """
    Execute a pandas operation using a cached DataFrame.

    Args:
        func (Callable): Operation function.
        csv_path (str): Path to the dataset.
        cached_df (Optional[pd.DataFrame]): Pre-loaded DataFrame.

    Returns:
        Any: Result of the operation, or None on failure.
    """
    try:
        df = cached_df if cached_df is not None else load_and_optimize_pandas(csv_path)
        return func(df)
    except Exception as e:
        print(f"Pandas-like operation failed: {e}")
        return None

def pandas_filter_group(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
    """
    Filter bytes > 1000, group by event_type, count rows.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pd.DataFrame]): Cached DataFrame.

    Returns:
        pd.Series: Grouped counts, or None if required columns missing.
    """
    def op(df: pd.DataFrame) -> Optional[pd.Series]:
        req = {"bytes", "event_type"}
        if not req.issubset(df.columns):
            return None
        filtered = df[df["bytes"] > 1000]
        return filtered.groupby("event_type", observed=False).size()
    return run_pandas_operation(op, csv_path, cached_df)

def pandas_statistics(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Group by event_type, compute mean/min/max for bytes, response_time_ms, risk_score.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pd.DataFrame]): Cached DataFrame.

    Returns:
        pd.DataFrame: Aggregated statistics, or None if required columns missing.
    """
    def op(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        req = {"event_type", "bytes", "response_time_ms", "risk_score"}
        if not req.issubset(df.columns):
            return None
        return df.groupby("event_type", observed=False).agg({
            "bytes": ["mean", "min", "max"],
            "response_time_ms": ["mean", "min", "max"],
            "risk_score": ["mean", "min", "max"]
        })
    return run_pandas_operation(op, csv_path, cached_df)

def pandas_complex_join(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pd.DataFrame]): Cached DataFrame.

    Returns:
        pd.DataFrame: Top 10 ranked rows, or None if required columns missing.
    """
    def op(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        req = {"source_ip", "bytes", "event_type"}
        if not req.issubset(df.columns):
            return None
        summary = df.groupby("source_ip", observed=False)["bytes"].sum().reset_index().rename(columns={"bytes": "total_bytes"})
        merged = df.merge(summary, on="source_ip", how="left")
        merged["total_rank"] = merged.groupby("event_type", observed=False)["total_bytes"].rank(method="dense", ascending=False)
        return merged.loc[merged["total_rank"] <= 10]
    return run_pandas_operation(op, csv_path, cached_df)

def pandas_timeseries(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
    """
    Extract hour from timestamp, group by (hour, event_type), count rows.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pd.DataFrame]): Cached DataFrame.

    Returns:
        pd.Series: Grouped counts, or None if required columns missing.
    """
    def op(df: pd.DataFrame) -> Optional[pd.Series]:
        if "event_type" not in df.columns:
            return None
        work = df.copy()
        if "timestamp" in work.columns:
            ts = pd.to_datetime(work["timestamp"], errors="coerce")
            hour = ts.dt.hour
        else:
            hour = 0
        work = work.assign(_hour=hour)
        return work.groupby(["_hour", "event_type"], observed=False).size()
    return run_pandas_operation(op, csv_path, cached_df)

# ----------------- Polars -----------------
def run_polars_operation(func: Callable[[pl.DataFrame], Any],
                         csv_path: str,
                         cached_df: Optional[pl.DataFrame] = None) -> Any:
    """
    Execute a Polars operation using a cached DataFrame.

    Args:
        func (Callable): Operation function.
        csv_path (str): Path to the dataset.
        cached_df (Optional[pl.DataFrame]): Pre-loaded DataFrame.

    Returns:
        Any: Result of the operation, or None on failure.
    """
    try:
        df = cached_df if cached_df is not None else _read_polars(csv_path)
        return func(df)
    except Exception as e:
        print(f"Polars operation failed: {e}")
        return None

def polars_filter_group(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """
    Filter bytes > 1000, group by event_type, count rows.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pl.DataFrame]): Cached DataFrame.

    Returns:
        pl.DataFrame: Grouped counts, or None if required columns missing.
    """
    def func(df: pl.DataFrame) -> Optional[pl.DataFrame]:
        if not {"bytes", "event_type"}.issubset(set(df.columns)):
            return None
        return df.filter(pl.col("bytes") > 1000).group_by("event_type").agg(pl.len().alias("count"))
    return run_polars_operation(func, csv_path, cached_df)

def polars_statistics(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """
    Group by event_type, compute mean/min/max for bytes, response_time_ms, risk_score.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pl.DataFrame]): Cached DataFrame.

    Returns:
        pl.DataFrame: Aggregated statistics, or None if required columns missing.
    """
    def func(df: pl.DataFrame) -> Optional[pl.DataFrame]:
        req = {"event_type", "bytes", "response_time_ms", "risk_score"}
        if not req.issubset(set(df.columns)):
            return None
        return df.group_by("event_type").agg([
            pl.col("bytes").mean().alias("bytes_mean"),
            pl.col("bytes").min().alias("bytes_min"),
            pl.col("bytes").max().alias("bytes_max"),
            pl.col("response_time_ms").mean().alias("response_time_ms_mean"),
            pl.col("response_time_ms").min().alias("response_time_ms_min"),
            pl.col("response_time_ms").max().alias("response_time_ms_max"),
            pl.col("risk_score").mean().alias("risk_score_mean"),
            pl.col("risk_score").min().alias("risk_score_min"),
            pl.col("risk_score").max().alias("risk_score_max")
        ])
    return run_polars_operation(func, csv_path, cached_df)

def polars_complex_join(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """
    Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pl.DataFrame]): Cached DataFrame.

    Returns:
        pl.DataFrame: Top 10 ranked rows, or None if required columns missing.
    """
    def func(df: pl.DataFrame) -> Optional[pl.DataFrame]:
        if not {"source_ip", "bytes", "event_type"}.issubset(set(df.columns)):
            return None
        summary = df.group_by("source_ip").agg(pl.col("bytes").sum().alias("total_bytes"))
        joined = df.join(summary, on="source_ip", how="left")
        ranked = joined.with_columns(
            pl.col("total_bytes").rank("dense", descending=True).over("event_type").alias("total_rank")
        )
        return ranked.filter(pl.col("total_rank") <= 10)
    return run_polars_operation(func, csv_path, cached_df)

def polars_timeseries(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """
    Extract hour from timestamp, group by (hour, event_type), count rows.

    Args:
        csv_path (str): Path to the dataset.
        cached_df (Optional[pl.DataFrame]): Cached DataFrame.

    Returns:
        pl.DataFrame: Grouped counts, or None if required columns missing.
    """
    def func(df: pl.DataFrame) -> Optional[pl.DataFrame]:
        if "event_type" not in df.columns:
            return None
        if "timestamp" in df.columns:
            df2 = df.with_columns(
                pl.col("timestamp").str.to_datetime(strict=False).dt.hour().alias("_hour")
            )
        else:
            df2 = df.with_columns(pl.lit(0).alias("_hour"))
        return df2.group_by(["_hour", "event_type"]).agg(pl.len().alias("count"))
    return run_polars_operation(func, csv_path, cached_df)

# ----------------- DuckDB -----------------
def duckdb_filter_group(csv_path: str) -> pd.DataFrame:
    """
    Filter bytes > 1000, group by event_type, count rows.

    Args:
        csv_path (str): Path to the dataset.

    Returns:
        pd.DataFrame: Grouped counts.
    """
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        return conn.execute(f"""
            SELECT event_type, COUNT(*) AS count
            FROM {source}
            WHERE bytes > 1000
            GROUP BY event_type
        """).fetchdf()
    return run_duckdb_operation(operation, csv_path)

def duckdb_statistics(csv_path: str) -> pd.DataFrame:
    """
    Group by event_type, compute mean/min/max for bytes, response_time_ms, risk_score.

    Args:
        csv_path (str): Path to the dataset.

    Returns:
        pd.DataFrame: Aggregated statistics.
    """
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        return conn.execute(f"""
            SELECT event_type,
                   AVG(bytes) AS bytes_mean, MIN(bytes) AS bytes_min, MAX(bytes) AS bytes_max,
                   AVG(response_time_ms) AS response_time_ms_mean,
                   MIN(response_time_ms) AS response_time_ms_min,
                   MAX(response_time_ms) AS response_time_ms_max,
                   AVG(risk_score) AS risk_score_mean,
                   MIN(risk_score) AS risk_score_min,
                   MAX(risk_score) AS risk_score_max
            FROM {source}
            GROUP BY event_type
        """).fetchdf()
    return run_duckdb_operation(operation, csv_path)

def duckdb_complex_join(csv_path: str) -> pd.DataFrame:
    """
    Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10.

    Args:
        csv_path (str): Path to the dataset.

    Returns:
        pd.DataFrame: Top 10 ranked rows.
    """
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        return conn.execute(f"""
            WITH summary AS (
                SELECT source_ip, SUM(bytes) AS total_bytes
                FROM {source}
                GROUP BY source_ip
            ),
            joined AS (
                SELECT d.*, s.total_bytes
                FROM {source} d
                JOIN summary s USING (source_ip)
            ),
            ranked AS (
                SELECT *, DENSE_RANK() OVER (
                    PARTITION BY event_type ORDER BY total_bytes DESC
                ) AS total_rank
                FROM joined
            )
            SELECT * FROM ranked WHERE total_rank <= 10
        """).fetchdf()
    return run_duckdb_operation(operation, csv_path)

def duckdb_timeseries(csv_path: str) -> pd.DataFrame:
    """
    Extract hour from timestamp, group by (hour, event_type), count rows.

    Args:
        csv_path (str): Path to the dataset.

    Returns:
        pd.DataFrame: Grouped counts.
    """
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        try:
            return conn.execute(f"""
                SELECT date_part('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
                       event_type,
                       COUNT(*) AS count
                FROM {source}
                GROUP BY hour, event_type
            """).fetchdf()
        except Exception:
            return conn.execute(f"""
                SELECT 0 AS hour, event_type, COUNT(*) AS count
                FROM {source}
                GROUP BY event_type
            """).fetchdf()
    return run_duckdb_operation(operation, csv_path)

# ----------------- FireDucks (optional) -----------------
if FIREDUCKS_AVAILABLE:
    def fireducks_filter_group(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
        """
        Filter bytes > 1000, group by event_type, count rows (FireDucks version).

        Args:
            csv_path (str): Path to the dataset.
            cached_df (Optional[pd.DataFrame]): Cached DataFrame.

        Returns:
            pd.Series: Grouped counts, or None if required columns missing.
        """
        def op(df: pd.DataFrame) -> Optional[pd.Series]:
            if not {"bytes", "event_type"}.issubset(df.columns):
                return None
            return df[df["bytes"] > 1000].groupby("event_type").size()
        return run_pandas_operation(op, csv_path, cached_df)

    def fireducks_statistics(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Group by event_type, compute mean/min/max for bytes, response_time_ms, risk_score (FireDucks version).

        Args:
            csv_path (str): Path to the dataset.
            cached_df (Optional[pd.DataFrame]): Cached DataFrame.

        Returns:
            pd.DataFrame: Aggregated statistics, or None if required columns missing.
        """
        def op(df: pd.DataFrame) -> Optional[pd.DataFrame]:
            req = {"event_type", "bytes", "response_time_ms", "risk_score"}
            if not req.issubset(df.columns):
                return None
            return df.groupby("event_type").agg({
                "bytes": ["mean", "min", "max"],
                "response_time_ms": ["mean", "min", "max"],
                "risk_score": ["mean", "min", "max"]
            })
        return run_pandas_operation(op, csv_path, cached_df)

    def fireducks_complex_join(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10 (FireDucks version).

        Args:
            csv_path (str): Path to the dataset.
            cached_df (Optional[pd.DataFrame]): Cached DataFrame.

        Returns:
            pd.DataFrame: Top 10 ranked rows, or None if required columns missing.
        """
        def op(df: pd.DataFrame) -> Optional[pd.DataFrame]:
            req = {"source_ip", "bytes", "event_type"}
            if not req.issubset(df.columns):
                return None
            summary = df.groupby("source_ip")["bytes"].sum().reset_index().rename(columns={"bytes": "total_bytes"})
            merged = df.merge(summary, on="source_ip", how="left")
            merged["total_rank"] = merged.groupby("event_type")["total_bytes"].rank(method="dense", ascending=False)
            return merged[merged["total_rank"] <= 10]
        return run_pandas_operation(op, csv_path, cached_df)

    def fireducks_timeseries(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
        """
        Extract hour from timestamp, group by (hour, event_type), count rows (FireDucks version).

        Args:
            csv_path (str): Path to the dataset.
            cached_df (Optional[pd.DataFrame]): Cached DataFrame.

        Returns:
            pd.Series: Grouped counts, or None if required columns missing.
        """
        def op(df: pd.DataFrame) -> Optional[pd.Series]:
            if "event_type" not in df.columns:
                return None
            if "timestamp" in df.columns:
                ts = pd.to_datetime(df["timestamp"], errors="coerce")
                hour = ts.dt.hour
            else:
                hour = 0
            tmp = df.assign(_hour=hour)
            return tmp.groupby(["_hour", "event_type"]).size()
        return run_pandas_operation(op, csv_path, cached_df)

def run_benchmark_operation(library_name: str,
                            operation_func: Callable,
                            operation_name: str,
                            csv_path: str) -> Optional[float]:
    """
    Time a single (library, operation) pair.

    Args:
        library_name (str): Name of the library.
        operation_func (Callable): Function to execute.
        operation_name (str): Name of the operation.
        csv_path (str): Path to the dataset.

    Returns:
        Optional[float]: Duration in seconds, or None on failure.
    """
    log_memory_usage(f"{library_name} {operation_name} (start)")
    start = time.perf_counter()
    result = operation_func(csv_path)
    duration = time.perf_counter() - start
    log_memory_usage(f"{library_name} {operation_name} (end)")
    print(f"{library_name} {operation_name} duration: {duration:.6f}s")
    gc.collect()
    if result is None:
        return None
    return max(duration, 1e-6)

def load_and_optimize_fireducks(csv_path: str) -> pd.DataFrame:
    """
    Load and optimize a FireDucks DataFrame once.

    Args:
        csv_path (str): Path to the file.

    Returns:
        pd.DataFrame: Optimized DataFrame.

    Raises:
        RuntimeError: If FireDucks is not available.
    """
    if not FIREDUCKS_AVAILABLE:
        raise RuntimeError("FireDucks not available")
    if csv_path.endswith('.parquet'):
        df = fpd.read_parquet(csv_path)
    else:
        df = fpd.read_csv(csv_path)
    try:
        original = df.memory_usage(deep=True).sum()
        opt = optimize_benchmark_df(df)
        optimized = opt.memory_usage(deep=True).sum()
        red = (original - optimized) / original * 100 if original > 0 else 0
        print(f"  fireducks DataFrame optimized: {red:.1f}% memory reduction ({original/1024/1024:.1f}MB → {optimized/1024/1024:.1f}MB)")
        del df
        gc.collect()
        return opt
    except Exception as e:
        print(f"Warning: FireDucks optimization failed: {e}")
        return df

def run_library_benchmarks(library_name: str, csv_path: str, repeat: int = 1) -> dict:
    """
    Execute all benchmark operations for a single library with single-load caching.

    Args:
        library_name (str): Name of the library.
        csv_path (str): Path to the dataset.
        repeat (int): Number of repeats per operation.

    Returns:
        dict: Mapping of operation names to average durations.
    """
    operations = {}
    for op in ["filter_group", "statistics", "complex_join", "timeseries"]:
        fn = f"{library_name}_{op}"
        if fn in globals():
            operations[op] = globals()[fn]
        else:
            print(f"ERROR: Missing function {fn}; skipping {library_name}.")
            return {}

    cached_df = None
    if library_name == "pandas":
        print("Loading and optimizing pandas DataFrame...")
        cached_df = load_and_optimize_pandas(csv_path)
    elif library_name == "fireducks" and FIREDUCKS_AVAILABLE:
        print("Loading and optimizing fireducks DataFrame...")
        cached_df = load_and_optimize_fireducks(csv_path)
    elif library_name == "polars":
        print("Loading Polars DataFrame once...")
        cached_df = _read_polars(csv_path)

    out: dict = {}
    for op_name, op_func in operations.items():
        print(f"\n--- {library_name.upper()} {op_name} ---")
        durations: List[float] = []
        for _ in range(repeat):
            if library_name in ("pandas", "fireducks", "polars"):
                dur = run_benchmark_operation(
                    library_name,
                    lambda path, _op=op_func, _cache=cached_df: _op(path, _cache),
                    op_name,
                    csv_path
                )
            else:
                dur = run_benchmark_operation(library_name, op_func, op_name, csv_path)
            if dur is not None:
                durations.append(dur)
        out[op_name] = float(np.mean(durations)) if durations else None

    if cached_df is not None:
        print(f"  Releasing cached {library_name} DataFrame...")
        del cached_df
        gc.collect()

    return out

def run_all_benchmarks(csv_path: str, repeat: int = 1) -> dict:
    """
    Run benchmarks for all available libraries.

    Args:
        csv_path (str): Path to the dataset.
        repeat (int): Number of repeats.

    Returns:
        dict: Results per library.
    """
    libraries = ["pandas", "polars", "duckdb"]
    if FIREDUCKS_AVAILABLE:
        libraries.append("fireducks")
    results: dict = {}
    for lib in libraries:
        print(f"\n{'='*50}\nRunning benchmarks for {lib.upper()}...\n{'='*50}")
        lib_results = run_library_benchmarks(lib, csv_path, repeat)
        if lib_results:
            results[lib] = lib_results
    return results

# Host info and CSV functions remain the same...
def normalize_host_info(raw: dict) -> dict:
    """
    Normalize host_info dict to expected keys with safe fallbacks.

    Args:
        raw (dict): Raw host info.

    Returns:
        dict: Normalized host info.
    """
    EXPECTED_HOST_FIELDS = [
        "hostname","os_version","platform","os_major","os_minor","os_build",
        "architecture","processor","logical_cores","physical_cores",
        "max_frequency_mhz","current_frequency_mhz",
        "total_memory_gb","available_memory_gb",
        "python_version","python_implementation","cpu_brand","cpu_arch"
    ]
    
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
        if k in alt_map:
            val = None
            for alt in alt_map[k]:
                if alt in raw:
                    val = raw[alt]
                    break
            norm[k] = val if val is not None else ""
        else:
            norm[k] = raw.get(k, "")

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
    """
    Gather system metrics with robust fallbacks.

    Returns:
        dict: Fallback system info.
    """
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
    Map internal host_info to legacy CSV schema.

    Args:
        host_info (dict): Host info dict.

    Returns:
        dict: Legacy normalized dict.
    """
    fallbacks = _collect_system_fallbacks()
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

def save_results_to_csv(results: dict, host_info: dict, script_name: str,
                        dataset_size: int, output_path: Union[str, Path]) -> None:
    """
    Save benchmark results to CSV with blank cells for missing library timings.
    Always includes all possible library columns for consistency, even if unavailable.
    
    Args:
        results (dict): Benchmark results per library.
        host_info (dict): Host information.
        script_name (str): Name of the script.
        dataset_size (int): Size of the dataset.
        output_path (Union[str, Path]): Output file path.
    """
    host_order = [
        "timestamp", "hostname", "platform", "system", "release", "version", "machine", "processor",
        "cpu_count_logical", "cpu_count_physical", "cpu_freq_max", "cpu_freq_current",
        "memory_total_gb", "memory_available_gb", "python_version", "python_implementation",
        "cpu_brand", "cpu_arch"
    ]
    operations = ["filter_group", "statistics", "complex_join", "timeseries"]
    
    # FIXED: Always include ALL possible libraries, even if unavailable (for consistent CSV header)
    libs = ["pandas", "polars", "duckdb", "fireducks"]
    
    timing_keys = [f"{op}_{lib}_seconds" for op in operations for lib in libs]
    legacy_host = _normalize_host_info_legacy(host_info)
    timestamp = datetime.now().isoformat()

    try:
        ds_path = Path(output_path).parent.parent / "raw" / "synthetic_logs_10M.csv"
        ds_name = ds_path.name
        ds_fmt = "csv"
    except Exception:
        ds_name = "unknown"
        ds_fmt = "unknown"

    row = [timestamp] + [legacy_host.get(k, "") for k in host_order[1:]] + [dataset_size, ds_name, ds_fmt]

    for op in operations:
        for lib in libs:
            val = results.get(lib, {}).get(op)
            if val is None or (isinstance(val, float) and (math.isnan(val) or val <= 0)):
                row.append("")  # Blank for missing/unavailable
            else:
                # FIXED: Use high precision to match benchmark.py (15 decimal places)
                row.append(f"{val}")

    row.append(script_name)

    header = host_order + ["dataset_size", "dataset_name", "dataset_format"] + timing_keys + ["script_name"]

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not out_file.exists()
    with open(out_file, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerow(row)

def print_summary(results: dict) -> None:
    """
    Print a comparative summary of benchmark timings.

    Args:
        results (dict): Benchmark results.
    """
    print("\n" + "="*50)
    print("BENCHMARK SUMMARY")
    print("="*50 + "\n")
    operations = ["filter_group", "statistics", "complex_join", "timeseries"]
    libraries = ["pandas", "polars", "duckdb" ]
    if FIREDUCKS_AVAILABLE:
        libraries.append("fireducks")
    
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
        #print(f"  Fastest: {fastest_lib} ({fastest_time:.6f}s)")
        for lib, t in sorted(timings.items(), key=lambda x: x[1]):
            #factor = fastest_time / t if t > 0 else 0  # FIXED: Avoid division by zero
            #factor = t / fastest_time 
            dif = t - fastest_time
            if lib == fastest_lib:
                print(f"  {lib:<10}: {t:7.6f}s (Fastest)")    
            else:
                print(f"  {lib:<10}: {t:7.6f}s ({dif:,.4f}s more than {fastest_lib})")
        print()

def main():
    """
    CLI entrypoint for running the full benchmark workflow.
    """
    parser = argparse.ArgumentParser(description="Comprehensive Data Processing Benchmark")
    parser.add_argument("-d","--dataset", type=str, help="Path to dataset file")
    parser.add_argument("-o","--output", type=str, help="Output CSV file path")
    parser.add_argument("-r","--repeat", type=int, default=1, help="Repeat count per operation")
    args = parser.parse_args()

    dataset_path = Path(args.dataset) if args.dataset else Path("data/raw/synthetic_logs_test.csv")
    output_path = Path(args.output) if args.output else Path("data/benchmark_results.csv")

    print("="*60)
    print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
    print("="*60)
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
    save_results_to_csv(results, raw_host_info, "benchmark_01.py", dataset_size, str(output_path))
    print(f"Results saved to {output_path}")

    print_summary(results)
    print("Benchmark completed!")

if __name__ == "__main__":
    main()