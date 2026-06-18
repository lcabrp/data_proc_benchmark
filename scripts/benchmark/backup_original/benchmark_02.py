# benchmark_02.py - Comprehensive Data Processing Benchmark (aligned with benchmark.py and benchmark_01.py)
import time
import pandas as pd
# Enable global Pandas 3.0 future defaults for optimized memory and speed
pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True
import polars as pl
import duckdb
import warnings
import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
import os
import gzip
import json
import argparse
import numpy as np
import gc
from typing import Optional, Callable, Any, Union, List
from contextlib import redirect_stderr
from typing import cast
from pathlib import Path

# Suppress noisy SyntaxWarnings (e.g. invalid escape sequence '\_')
warnings.filterwarnings("ignore", category=SyntaxWarning, message=r"invalid escape sequence \\_")

# Add the project root to Python path for utils import
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from utils.host_info import get_host_info  # noqa: E402
from utils.config import setup_project  # noqa: E402
from utils.data_io import get_dataset_size as universal_dataset_size  # noqa: E402
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
from utils.duckdb_utils import DuckDBBenchmarkSource, duckdb_table_expr  # noqa: E402
from utils.pandas_benchmark_ops import complex_join_top_ranked, timeseries_hour_counts  # noqa: E402
from utils.useful_functions import optimize_df_types  # noqa: E402
from utils.platform_utils import FIREDUCKS_AVAILABLE  # noqa: E402

if FIREDUCKS_AVAILABLE:
    import fireducks.pandas as fpd
else:
    fpd = None

def _print_prep_timing(library: str, step: str, start_time: float) -> None:
    """Print elapsed time for a dataset preparation step."""
    print_prep_timing(library, step, start_time)


def _should_optimize_for_library(library: str) -> bool:
    """Return the shared memory optimization decision and print it once."""
    decision = decide_memory_optimization(OPTIMIZE_MODE, MEMORY_THRESHOLD_GB)
    if library not in _OPTIMIZATION_MESSAGE_PRINTED:
        print(f"  {decision.message}")
        _OPTIMIZATION_MESSAGE_PRINTED.add(library)
    return decision.should_optimize


BENCHMARK_OPTIMIZATION_TYPES = {
    'datetime64[ns]': ['timestamp'],
    'category': ['source_ip', 'destination_ip', 'protocol', 'event_type',
                 'severity', 'user', 'status_code', 'country', 'device_type'],
    'uint32': ['bytes', 'session_id'],
    'uint16': ['response_time_ms', 'port'],
    'float32': ['risk_score']
}
PREP_MEMORY_REPORT = "off"
OPTIMIZED_CACHE_MODE = "off"
OPTIMIZED_CACHE_DIR = Path(project_root) / "data" / "cache" / "optimized"
USE_CSV_DTYPE_HINTS = True
OPTIMIZE_MODE = "auto"
MEMORY_THRESHOLD_GB = 16.0
_OPTIMIZATION_MESSAGE_PRINTED: set[str] = set()
DUCKDB_SOURCE = DuckDBBenchmarkSource()

# File format detection and universal reading functions
def detect_file_format(file_path: Path) -> str:
    """Detect the file format based on extension and content."""
    suffixes = file_path.suffixes
    
    # Handle compressed files
    if '.gz' in suffixes or '.zip' in suffixes or '.zst' in suffixes:
        # Get the format before compression
        clean_suffixes = [s for s in suffixes if s not in ['.gz', '.zip', '.zst']]
        if clean_suffixes:
            format_ext = clean_suffixes[-1]
        else:
            format_ext = '.csv'  # Default assumption
    else:
        format_ext = file_path.suffix
    
    format_map = {
        '.csv': 'csv',
        '.parquet': 'parquet', 
        '.json': 'json',
        '.jsonl': 'ndjson',
        '.ndjson': 'ndjson'
    }
    
    return format_map.get(format_ext.lower(), 'csv')

def read_file_universal(file_path: Path, library: str = 'pandas', **kwargs) -> Union[pd.DataFrame, pl.DataFrame, Any]:
    """Universal file reader that handles multiple formats and libraries."""
    file_format = detect_file_format(file_path)
    
    # Handle compressed files
    if any(ext in file_path.suffixes for ext in ['.gz', '.zip', '.zst']):
        if '.gz' in file_path.suffixes:
            if library == 'pandas':
                if file_format == 'csv':
                    return pd.read_csv(file_path, compression='gzip', **kwargs)
                elif file_format == 'json':
                    with gzip.open(file_path, 'rt') as f:
                        data = [json.loads(line) for line in f]
                    return pd.DataFrame(data)
            elif library == 'polars':
                if file_format == 'csv':
                    return pl.read_csv(file_path, **kwargs)
                elif file_format == 'json':
                    return pl.read_ndjson(file_path)
        # Add other compression handling as needed
    
    # Standard file reading
    if library == 'pandas':
        if file_format == 'csv':
            return pd.read_csv(file_path, **kwargs)
        elif file_format == 'parquet':
            return pd.read_parquet(file_path, **kwargs)
        elif file_format == 'json':
            return pd.read_json(file_path, **kwargs)
        elif file_format == 'ndjson':
            return pd.read_json(file_path, lines=True, **kwargs)
    elif library == 'polars':
        if file_format == 'csv':
            return pl.read_csv(file_path, **kwargs)
        elif file_format == 'parquet':
            return pl.read_parquet(file_path, **kwargs) 
        elif file_format == 'json':
            return pl.read_json(file_path, **kwargs)
        elif file_format == 'ndjson':
            return pl.read_ndjson(file_path, **kwargs)
    
    # Fallback to pandas CSV
    return pd.read_csv(file_path, **kwargs)

# Use pathlib for cross-platform paths
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Adjust based on script location

# Auto-detect the best available dataset file
def find_dataset_file() -> Path:
    """Find the best available dataset file in order of preference."""
    data_dir = PROJECT_ROOT / "data" / "raw"
    
    # Preference order: Parquet > CSV > NDJSON > JSON > Compressed variants
    candidates = [
        "synthetic_logs_7M.parquet",
        "synthetic_logs_7M.csv", 
        "synthetic_logs_10M.parquet",
        "synthetic_logs_10M.csv",
        "synthetic_logs_test.csv",
        "synthetic_logs.parquet",
        "synthetic_logs.csv",
        "synthetic_logs.ndjson",
        "synthetic_logs.jsonl",
        # Compressed variants
        "synthetic_logs_7M.csv.gz",
        "synthetic_logs_10M.csv.gz",
        "synthetic_logs.csv.gz",
        "logs.csv.gz",
    ]
    
    for filename in candidates:
        filepath = data_dir / filename
        if filepath.exists():
            print(f"Using dataset: {filepath}")
            return filepath
    
    # Fallback to the original expectation
    fallback = data_dir / "raw" / "synthetic_logs_7M.parquet"
    print(f"No dataset found, expecting: {fallback}")
    return fallback

def get_dataset_size(file_path: Path) -> int:
    """
    Get the number of records in the dataset by reading the file.
    Args:
        file_path (Path): Path to the dataset file.
    Returns:
        int: Number of records in the dataset.
    """
    try:
        return universal_dataset_size(Path(file_path))
    except Exception as e:
        print(f"Warning: Could not determine dataset size: {e}")
        return 0

# Optimization for pandas/fireducks
def optimize_benchmark_df(bdf: pd.DataFrame) -> pd.DataFrame:
    """
    Apply custom dtype optimization rules for memory efficiency.
    Matches benchmark.py optimizations.
    """
    return optimize_df_types(bdf, BENCHMARK_OPTIMIZATION_TYPES)

def load_and_optimize_pandas(csv_path: str) -> pd.DataFrame:
    """
    Load and optimize pandas DataFrame once.
    """
    df = load_pandas_like_for_benchmark(
        Path(csv_path),
        library="pandas",
        type_map=BENCHMARK_OPTIMIZATION_TYPES,
        should_optimize=_should_optimize_for_library("pandas"),
        prep_memory_report=PREP_MEMORY_REPORT,
        optimized_cache_mode=OPTIMIZED_CACHE_MODE,
        optimized_cache_dir=OPTIMIZED_CACHE_DIR,
        use_dtype_hints=USE_CSV_DTYPE_HINTS,
    )
    step_start = time.perf_counter()
    gc.collect()
    _print_prep_timing("pandas", "gc after optimization", step_start)
    return df

def _read_polars(path: str) -> pl.DataFrame:
    """
    Read dataset into Polars DataFrame.
    """
    fmt = detect_file_format(Path(path))
    if fmt == 'csv':
        return cast(pl.DataFrame, pl.read_csv(path))
    elif fmt == 'parquet':
        return cast(pl.DataFrame, pl.read_parquet(path))
    elif fmt == 'json':
        return cast(pl.DataFrame, pl.read_json(path))
    elif fmt == 'ndjson':
        try:
            return cast(pl.DataFrame, pl.read_ndjson(path))
        except Exception:
            return cast(pl.DataFrame, pl.read_json(path, lines=True))
    else:
        return cast(pl.DataFrame, pl.read_csv(path))

def load_and_optimize_fireducks(csv_path: str) -> pd.DataFrame:
    """
    Load and optimize FireDucks DataFrame once.
    """
    if not FIREDUCKS_AVAILABLE:
        raise RuntimeError("FireDucks not available")
    df = load_pandas_like_for_benchmark(
        Path(csv_path),
        library="fireducks",
        type_map=BENCHMARK_OPTIMIZATION_TYPES,
        should_optimize=_should_optimize_for_library("fireducks"),
        prep_memory_report=PREP_MEMORY_REPORT,
        optimized_cache_mode=OPTIMIZED_CACHE_MODE,
        optimized_cache_dir=OPTIMIZED_CACHE_DIR,
        use_dtype_hints=USE_CSV_DTYPE_HINTS,
    )
    step_start = time.perf_counter()
    gc.collect()
    _print_prep_timing("fireducks", "gc after optimization", step_start)
    return df

# Helper functions for single-load caching
def run_pandas_operation(func: Callable[[pd.DataFrame], Any],
                         csv_path: str,
                         cached_df: Optional[pd.DataFrame] = None) -> Any:
    """
    Execute pandas operation using cached DataFrame.
    """
    try:
        df = cached_df if cached_df is not None else load_and_optimize_pandas(csv_path)
        return func(df)
    except Exception as e:
        print(f"Pandas operation failed: {e}")
        return None

def run_polars_operation(func: Callable[[pl.DataFrame], Any],
                         csv_path: str,
                         cached_df: Optional[pl.DataFrame] = None) -> Any:
    """
    Execute Polars operation using cached DataFrame.
    """
    try:
        df = cached_df if cached_df is not None else _read_polars(csv_path)
        return func(df)
    except Exception as e:
        print(f"Polars operation failed: {e}")
        return None

# Replace the DuckDB functions with self-contained versions

def run_duckdb_operation(operation_func: Callable, csv_path: str) -> Any:
    """Execute DuckDB operation with proper connection management."""
    try:
        with DUCKDB_SOURCE.query(csv_path) as (conn, source):
            return operation_func(source, conn)
    except Exception as e:
        print(f"DuckDB operation failed: {e}")
        return None


def _duckdb_source(path_or_expr: str) -> str:
    if path_or_expr == DUCKDB_SOURCE.table_name or path_or_expr.startswith("read_"):
        return path_or_expr
    return duckdb_table_expr(path_or_expr)

def duckdb_filter_group(csv_path: str, cached_df: Optional[Any] = None) -> pd.DataFrame:
    """Filter bytes > 1000, group by event_type, count. (cached_df unused for consistency)"""
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
        # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
        return conn.execute(f"""
            SELECT event_type, COUNT(*) AS count
            FROM {source}
            WHERE bytes > 1000
            GROUP BY event_type
        """).fetch_arrow_table()
    return run_duckdb_operation(operation, csv_path)

def duckdb_statistics(csv_path: str, cached_df: Optional[Any] = None) -> pd.DataFrame:
    """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score. (cached_df unused for consistency)"""
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
        # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
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
        """).fetch_arrow_table()
    return run_duckdb_operation(operation, csv_path)

def duckdb_complex_join(csv_path: str, cached_df: Optional[Any] = None) -> pd.DataFrame:
    """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10. (cached_df unused for consistency)"""
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
        # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
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
        """).fetch_arrow_table()
    return run_duckdb_operation(operation, csv_path)

def duckdb_timeseries(csv_path: str, cached_df: Optional[Any] = None) -> pd.DataFrame:
    """Extract hour from timestamp, group by (hour, event_type), count. (cached_df unused for consistency)"""
    def operation(path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        source = _duckdb_source(path)
        # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
        # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
        try:
            return conn.execute(f"""
                SELECT date_part('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
                       event_type,
                       COUNT(*) AS count
                FROM {source}
                GROUP BY hour, event_type
            """).fetch_arrow_table()
        except Exception:
            return conn.execute(f"""
                SELECT 0 AS hour, event_type, COUNT(*) AS count
                FROM {source}
                GROUP BY event_type
            """).fetch_arrow_table()
    return run_duckdb_operation(operation, csv_path)

# Operation 1: Filter and Group (aligned with benchmark.py) - CACHING AWARE ONLY
def pandas_filter_group(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
    """Filter bytes > 1000, group by event_type, count."""
    def op(df: pd.DataFrame) -> Optional[pd.Series]:
        if "bytes" not in df.columns or "event_type" not in df.columns:
            return None
        filtered = df[df["bytes"] > 1000]
        return filtered.groupby("event_type", observed=False).size()
    return run_pandas_operation(op, csv_path, cached_df)

def polars_filter_group(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Filter bytes > 1000, group by event_type, count."""
    def func(df: pl.DataFrame) -> Optional[pl.DataFrame]:
        if not {"bytes", "event_type"}.issubset(set(df.columns)):
            return None
        return df.filter(pl.col("bytes") > 1000).group_by("event_type").agg(pl.len().alias("count"))
    return run_polars_operation(func, csv_path, cached_df)

def fireducks_filter_group(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
    """Filter bytes > 1000, group by event_type, count."""
    def op(df: pd.DataFrame) -> Optional[pd.Series]:
        if "bytes" not in df.columns or "event_type" not in df.columns:
            return None
        return df[df["bytes"] > 1000].groupby("event_type").size()
    return run_pandas_operation(op, csv_path, cached_df)

# Operation 2: Statistical Analysis (aligned with benchmark.py) - CACHING AWARE ONLY
def pandas_statistics(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score."""
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

def polars_statistics(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score."""
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

def fireducks_statistics(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score."""
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

# Operation 3: Complex Join (aligned with benchmark.py) - CACHING AWARE ONLY
def pandas_complex_join(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
    def op(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        return complex_join_top_ranked(df, rank_col="total_rank", observed=False)
    return run_pandas_operation(op, csv_path, cached_df)

def polars_complex_join(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
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

def fireducks_complex_join(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
    def op(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        return complex_join_top_ranked(df, rank_col="total_rank", observed=False)
    return run_pandas_operation(op, csv_path, cached_df)

# Operation 4: Time Series (aligned with benchmark.py) - CACHING AWARE ONLY
def pandas_timeseries(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
    """Extract hour from timestamp, group by (hour, event_type), count."""
    def op(df: pd.DataFrame) -> Optional[pd.Series]:
        return timeseries_hour_counts(df, observed=False, reset_index=False, hour_name="_hour")
    return run_pandas_operation(op, csv_path, cached_df)

def polars_timeseries(csv_path: str, cached_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Extract hour from timestamp, group by (hour, event_type), count."""
    def func(df: pl.DataFrame) -> Optional[pl.DataFrame]:
        if "event_type" not in df.columns:
            return None
        if "timestamp" in df.columns:
            if df["timestamp"].dtype == pl.Utf8:
                hour_expr = pl.col("timestamp").str.slice(11, 2).cast(pl.UInt8)
            else:
                hour_expr = pl.col("timestamp").dt.hour()
            df2 = df.with_columns(hour_expr.alias("_hour"))
        else:
            df2 = df.with_columns(pl.lit(0).alias("_hour"))
        return df2.group_by(["_hour", "event_type"]).agg(pl.len().alias("count"))
    return run_polars_operation(func, csv_path, cached_df)

def fireducks_timeseries(csv_path: str, cached_df: Optional[pd.DataFrame] = None) -> pd.Series:
    """Extract hour from timestamp, group by (hour, event_type), count."""
    def op(df: pd.DataFrame) -> Optional[pd.Series]:
        return timeseries_hour_counts(df, observed=False, reset_index=False, hour_name="_hour")
    return run_pandas_operation(op, csv_path, cached_df)

# Benchmark runner with single-load caching - LIBRARY BY LIBRARY EXECUTION
def run_library_benchmarks(library_name: str, csv_path: str, repeat: int = 1) -> dict:
    """Execute all benchmark operations for a single library with single-load caching."""
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
        prep_start = time.perf_counter()
        cached_df = _read_polars(csv_path)
        _print_prep_timing("polars", "read/load", prep_start)
        _print_prep_timing("polars", "total load/optimization", prep_start)
    elif library_name == "duckdb":
        prep_start = time.perf_counter()
        elapsed = DUCKDB_SOURCE.prepare(csv_path)
        if elapsed is None:
            record_prep_timing("duckdb", "total load/optimization", 0.0)
            print("  [duckdb prep] total load/optimization: 0.000s (file scan mode)")
        else:
            record_prep_timing("duckdb", "read/load cached table", elapsed)
            print(f"  [duckdb prep] read/load cached table: {elapsed:.3f}s")
            _print_prep_timing("duckdb", "total load/optimization", prep_start)

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
        step_start = time.perf_counter()
        gc.collect()
        _print_prep_timing(library_name, "gc after release", step_start)
    if library_name == "duckdb":
        DUCKDB_SOURCE.close()

    return out

def run_benchmark_operation(library_name: str,
                            operation_func: Callable,
                            operation_name: str,
                            csv_path: str) -> Optional[float]:
    """Time a single operation with proper FireDucks handling."""
    try:
        # Check if this is a FireDucks operation when FireDucks is not available
        if library_name.lower() == "fireducks" and not FIREDUCKS_AVAILABLE:
            print(f"{library_name} {operation_name} duration: 0.00s")
            return 0.0  # Return 0.0 for unavailable libraries
        
        start = time.perf_counter()
        result = operation_func(csv_path)
        duration = time.perf_counter() - start
        
        print(f"{library_name} {operation_name} duration: {duration:.4f}s")
        
        # If operation failed, return None (not 0.0)
        if result is None:
            return None
        
        return duration  # Return actual duration, no artificial minimum
        
    except Exception as e:
        print(f"{library_name} {operation_name} failed: {e}")
        return None  # Failed operations return None

def run_all_benchmarks(csv_path: str, repeat: int = 1) -> dict:
    """Run benchmarks using library-by-library execution with proper caching."""
    libraries = ["pandas", "polars", "duckdb"]
    if FIREDUCKS_AVAILABLE:
        libraries.append("fireducks")
    
    # Store results by library, then reorganize by operation
    library_results = {}
    
    for library_name in libraries:
        print(f"\n{'='*50}")
        print(f"Running {library_name.upper()} benchmarks...")
        print(f"{'='*50}")
        
        lib_results = run_library_benchmarks(library_name, csv_path, repeat)
        if lib_results:
            library_results[library_name] = lib_results
    
    # Reorganize results by operation -> library (to maintain output format)
    results = {}
    operations = ["filter_group", "statistics", "complex_join", "timeseries"]
    
    for operation in operations:
        results[operation] = {}
        for library_name in libraries:
            if library_name in library_results:
                results[operation][library_name] = library_results[library_name].get(operation)
            else:
                results[operation][library_name] = None
    
    return results

# CSV saving function - NO SAFE_VALUE NEEDED
def save_results_to_csv(results: dict, host_info: dict, script_name: str, dataset_size: int) -> None:
    """
    Save benchmark results to CSV file with error handling.
    Matches the structure of benchmark.py and benchmark_01.py.
    Args:
        results (dict): Benchmark results.
        host_info (dict): Host system information.
        script_name (str): Name of the script creating the record.
        dataset_size (int): Number of records in the dataset.
    """
    try:
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

        ds_name = DATASET_PATH.name if DATASET_PATH else 'unknown'
        ds_fmt = detect_file_format(DATASET_PATH) if DATASET_PATH else 'unknown'

        row = [
            host_info.get("timestamp"), host_info.get("hostname"), host_info.get("platform"),
            host_info.get("system"), host_info.get("release"), host_info.get("version"),
            host_info.get("machine"), host_info.get("processor"), host_info.get("cpu_count_logical"),
            host_info.get("cpu_count_physical"), host_info.get("cpu_freq_max"),
            host_info.get("cpu_freq_current"), host_info.get("memory_total_gb"),
            host_info.get("memory_available_gb"), host_info.get("python_version"),
            host_info.get("python_implementation"), host_info.get("cpu_brand"),
            host_info.get("cpu_arch"),
            dataset_size, ds_name, ds_fmt,
        ]

        for op in ["filter_group", "statistics", "complex_join", "timeseries"]:
            for lib in ["pandas", "polars", "duckdb", "fireducks"]:
                row.append(results.get(op, {}).get(lib))

        row.extend(get_prep_csv_values())
        row.append(script_name)
        append_csv_row_with_schema(RESULTS_CSV_PATH, header, row)
        print(f"Results saved to {RESULTS_CSV_PATH}")
    except Exception as e:
        print(f"Error saving results to CSV: {e}")

if __name__ == "__main__":
    try:
        # Use dynamic script name detection
        script_name = os.path.basename(__file__)  # This will be "benchmark_02.py"
        print(f"Running script: {script_name}")  # Debug print
        
        print("="*60)
        print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
        print("="*60)
        # Get host info with enhanced platform detection
        host_info = get_host_info()

        # Display key system information with WSL detection
        hostname = host_info.get('hostname', 'Unknown')
        system_info = host_info.get('system', 'Unknown')  # Now shows WSL2/WSL1
        cpu_brand = host_info.get('cpu_brand', 'Unknown')
        logical_cores = host_info.get('cpu_count_logical', 0)
        memory_total = host_info.get('memory_total_gb', 0.0)

        print(f"Running on: {hostname} ({system_info})")
        print(f"CPU: {cpu_brand} ({logical_cores} logical cores)")
        print(f"Memory: {memory_total:.2f} GB total")
        print()
        
        # Optional CLI/env dataset override
        parser = argparse.ArgumentParser(description="Comprehensive Data Processing Benchmark")
        parser.add_argument("-d", "--dataset", type=str, help="Path to the dataset file to benchmark")
        parser.add_argument("-o", "--output", type=str, help="Output CSV file path for results")
        parser.add_argument("--optimize", "-opt",
                            choices=["auto", "always", "never"],
                            default="auto",
                            help="Memory optimization mode: 'auto' (use threshold), 'always' (force), 'never' (disable) (default: auto)")
        parser.add_argument("--mem-threshold", "-m", type=float, default=16.0,
                            help="Memory threshold in GB for --optimize auto (default: 16)")
        parser.add_argument(
            "--prep-memory-report",
            choices=["off", "shallow", "deep"],
            default="off",
            help="Measure pandas/fireducks memory before/after optimization. Deep is expensive.",
        )
        parser.add_argument(
            "--optimized-cache",
            choices=["off", "read", "write", "readwrite", "refresh"],
            default="off",
            help="Use optimized pandas/fireducks parquet cache to skip repeated dtype optimization.",
        )
        parser.add_argument(
            "--optimized-cache-dir",
            type=Path,
            default=OPTIMIZED_CACHE_DIR,
            help="Directory for optimized pandas/fireducks cache files.",
        )
        parser.add_argument(
            "--no-csv-dtype-hints",
            action="store_true",
            help="Disable dtype hints while loading CSV with pandas/fireducks.",
        )
        parser.add_argument(
            "--duckdb-mode",
            choices=["file", "cached"],
            default="file",
            help="DuckDB file mode scans per operation; cached mode loads once into a temp table.",
        )
        args, _ = parser.parse_known_args()
        PREP_MEMORY_REPORT = args.prep_memory_report
        OPTIMIZED_CACHE_MODE = args.optimized_cache
        OPTIMIZED_CACHE_DIR = args.optimized_cache_dir
        USE_CSV_DTYPE_HINTS = not args.no_csv_dtype_hints
        OPTIMIZE_MODE = args.optimize
        MEMORY_THRESHOLD_GB = args.mem_threshold
        _OPTIMIZATION_MESSAGE_PRINTED = set()
        DUCKDB_SOURCE.set_mode(args.duckdb_mode)
        reset_prep_timings()

        dataset_env = os.environ.get("BENCHMARK_DATASET")
        chosen = args.dataset or dataset_env
        DATASET_PATH = None
        if chosen:
            DATASET_PATH = Path(chosen)
        if DATASET_PATH is None:
            DATASET_PATH = find_dataset_file()
        if DATASET_PATH is None or not DATASET_PATH.exists():
            raise RuntimeError("No dataset found. Use -d/--dataset or set BENCHMARK_DATASET.")

        # Determine output path
        config = setup_project()
        RESULTS_CSV_PATH = config.benchmark_results_file
        if args.output:
            RESULTS_CSV_PATH = Path(args.output)

        # Ensure directories exist
        DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
        RESULTS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Setup
        pd.set_option('display.float_format', '{:.0f}'.format)

        # Memory check before starting
        import psutil
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        if available_memory_gb < 8:
            print(f"Warning: Only {available_memory_gb:.1f}GB available memory. Benchmark may fail.")

        # Collect host information using the utils module
        print("Collecting host information...")
        host_info = get_host_info()
        print(f"Running on: {host_info.get('hostname', 'Unknown')} ({host_info.get('platform', 'Unknown')})")
        print(f"CPU: {host_info.get('cpu_brand', 'Unknown')} ({host_info.get('cpu_count_logical', 'N/A')} logical cores)")
        print(f"Memory: {host_info.get('memory_total_gb', 'N/A')} GB total")
        print("Preparation settings:")
        print(f"  - Optimization mode: {OPTIMIZE_MODE}")
        if OPTIMIZE_MODE == "auto":
            print(f"  - Auto optimization threshold: {MEMORY_THRESHOLD_GB}GB")
        print(f"  - Prep memory report: {PREP_MEMORY_REPORT}")
        print(f"  - Optimized cache: {OPTIMIZED_CACHE_MODE} ({OPTIMIZED_CACHE_DIR})")
        print(f"  - CSV dtype hints: {'enabled' if USE_CSV_DTYPE_HINTS else 'disabled'}")
        print(f"  - DuckDB mode: {args.duckdb_mode}")

        # Run all benchmarks
        print(f"\nStarting comprehensive benchmark with {DATASET_PATH}")
        print("This will test 4 different operations across 4 libraries...")
        
        # Get dataset size
        dataset_size = get_dataset_size(DATASET_PATH)
        print(f"Dataset size: {dataset_size:,} records")

        results = run_all_benchmarks(str(DATASET_PATH))

        # Save results to CSV
        print(f"\n{'='*50}")
        print("SAVING RESULTS")
        print(f"{'='*50}")
        decision = decide_memory_optimization(OPTIMIZE_MODE, MEMORY_THRESHOLD_GB)
        script_name = build_script_name(
            os.path.basename(__file__),
            OPTIMIZE_MODE,
            decision.should_optimize,
            decision.total_memory_gb,
        )
        save_results_to_csv(results, host_info, script_name, dataset_size)

        # Print summary - EXCLUDE ZERO DURATIONS FROM COMPARISON
        print(f"\n{'='*50}")
        print("BENCHMARK SUMMARY")
        print(f"{'='*50}")

        for operation, timings in results.items():
            print(f"\n{operation.upper()} Operation:")
            # Exclude None and zero durations from comparison (but keep in CSV)
            valid_timings = {lib: time for lib, time in timings.items() if time is not None and time > 0.0}
            if valid_timings:
                fastest = min(valid_timings.items(), key=lambda x: x[1])
                print(f"  Fastest: {fastest[0]} ({fastest[1]:.4f}s)")
                for lib, duration in sorted(valid_timings.items(), key=lambda x: x[1]):
                    speedup = fastest[1] / duration if duration > 0 else 0
                    print(f"  {lib:10}: {duration:6.4f}s (x{speedup:.4f})")
            else:
                print("  No valid timings to compare (all skipped or failed).")

        print(f"\nResults saved to: {RESULTS_CSV_PATH}")
        print("Benchmark completed!")

        # Suppress any remaining output
        with redirect_stderr(open(os.devnull, 'w')):
            time.sleep(0.1)  # Brief pause for cleanup
    except Exception as e:
        print(f"Critical error in main: {e}")
        # Ensure cleanup even on error
        try:
            if 'client' in globals():
                globals()['client'].close()
        except Exception:
            pass
        sys.exit(1)
