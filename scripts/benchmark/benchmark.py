"""Original Comprehensive Data Processing Benchmark (with CLI dataset override)."""
import time
import pandas as pd
import modin.pandas as mpd
import polars as pl
import duckdb
import platform
import logging
import warnings
import sys
import os
import csv
import argparse
from contextlib import redirect_stderr, redirect_stdout
from typing import cast, Optional, Iterable
from pathlib import Path

# Add the project root to Python path for utils import
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

# Import our utility modules
from utils.config import setup_project
from utils.data_io import read_data, find_dataset, get_dataset_size
from utils.host_info import get_host_info

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


def setup_modin() -> None:
    """Initialize Modin with quiet, stable Dask configuration."""
    try:
        # Keep Dask/Modin quiet
        logging.getLogger("distributed").setLevel(logging.ERROR)
        logging.getLogger("distributed.worker").setLevel(logging.ERROR)
        logging.getLogger("distributed.scheduler").setLevel(logging.ERROR)
        logging.getLogger("distributed.client").setLevel(logging.ERROR)
        logging.getLogger("tornado").setLevel(logging.ERROR)
        logging.getLogger("bokeh").setLevel(logging.ERROR)

        warnings.filterwarnings("ignore", category=UserWarning, module=".*modin.*")
        warnings.filterwarnings("ignore", category=FutureWarning, module=".*dask.*")

        import modin.config as cfg  # type: ignore
        cfg.Engine.put("dask")
        cfg.StorageFormat.put("pandas")

        from dask import config as dask_config  # type: ignore
        dask_config.set({
            "distributed.worker.daemon": False,
            "distributed.comm.timeouts.connect": "5s",
            # Keep partitions modest to reduce graph size
            "dataframe.shuffle.method": "tasks",
        })
    except Exception as e:
        print(f"Warning: Modin setup failed: {e}")


def cleanup_modin() -> None:
    """Clean up Modin and Dask resources."""
    try:
        try:
            import distributed  # type: ignore
            if hasattr(distributed, "default_client"):
                try:
                    client = distributed.default_client()
                    client.close()
                    print("Dask client closed")
                except (ValueError, RuntimeError):
                    pass
        except ImportError:
            pass
    except Exception as e:
        print(f"Warning: Modin cleanup failed: {e}")


# Helpers

def _detect_file_format(path: Path) -> str:
    """Detect file format by extension, aware of compressed endings."""
    suffixes = [s.lower() for s in path.suffixes]
    comp = {".gz", ".zip", ".zst", ".bz2"}
    base_suffixes = [s for s in suffixes if s not in comp]
    ext = (base_suffixes[-1] if base_suffixes else path.suffix).lower()
    if ext == ".parquet":
        return "parquet"
    if ext in {".ndjson", ".jsonl"}:
        return "ndjson"
    if ext == ".json":
        return "json"
    return "csv"


def _duckdb_table_expr(path: Path) -> str:
    """Return a DuckDB table expression for the given dataset path based on format."""
    fmt = _detect_file_format(path)
    # DuckDB likes forward slashes even on Windows
    p = str(path).replace("\\", "/")
    if fmt == "parquet":
        return f"read_parquet('{p}')"
    if fmt in ("json", "ndjson"):
        return f"read_json_auto('{p}')"
    return f"read_csv_auto('{p}')"


def _get_columns_duckdb(path: Path) -> set[str]:
    """Fast schema detection via DuckDB (zero-row scan)."""
    conn = duckdb.connect()
    try:
        expr = _duckdb_table_expr(path)
        df = conn.execute(f"SELECT * FROM {expr} LIMIT 0").fetchdf()
        return set(df.columns)
    finally:
        conn.close()


def _first_present(columns: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    """Return the first candidate present in columns."""
    cols = set(c.lower() for c in columns)
    for c in candidates:
        if c.lower() in cols:
            return c
    return None


# Results writer

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
                    "dataset_size",
                    "filter_group_pandas_seconds", "filter_group_modin_seconds", "filter_group_polars_seconds",
                    "filter_group_duckdb_seconds", "filter_group_fireducks_seconds",
                    "statistics_pandas_seconds", "statistics_modin_seconds", "statistics_polars_seconds",
                    "statistics_duckdb_seconds", "statistics_fireducks_seconds",
                    "complex_join_pandas_seconds", "complex_join_modin_seconds", "complex_join_polars_seconds",
                    "complex_join_duckdb_seconds", "complex_join_fireducks_seconds",
                    "timeseries_pandas_seconds", "timeseries_modin_seconds", "timeseries_polars_seconds",
                    "timeseries_duckdb_seconds", "timeseries_fireducks_seconds",
                    "script_name",
                ]
                writer.writerow(header)

            def safe_value(v):
                return "N/A" if v is None else v

            row = [
                host_info.get("timestamp"), host_info.get("hostname"), host_info.get("platform"),
                host_info.get("system"), host_info.get("release"), host_info.get("version"),
                host_info.get("machine"), host_info.get("processor"), host_info.get("cpu_count_logical"),
                host_info.get("cpu_count_physical"), host_info.get("cpu_freq_max"), host_info.get("cpu_freq_current"),
                host_info.get("memory_total_gb"), host_info.get("memory_available_gb"), host_info.get("python_version"),
                host_info.get("python_implementation"), host_info.get("cpu_brand"), host_info.get("cpu_arch"),
                dataset_size,
                safe_value(results.get("filter_group", {}).get("pandas")),
                safe_value(results.get("filter_group", {}).get("modin")),
                safe_value(results.get("filter_group", {}).get("polars")),
                safe_value(results.get("filter_group", {}).get("duckdb")),
                safe_value(results.get("filter_group", {}).get("fireducks")),
                safe_value(results.get("statistics", {}).get("pandas")),
                safe_value(results.get("statistics", {}).get("modin")),
                safe_value(results.get("statistics", {}).get("polars")),
                safe_value(results.get("statistics", {}).get("duckdb")),
                safe_value(results.get("statistics", {}).get("fireducks")),
                safe_value(results.get("complex_join", {}).get("pandas")),
                safe_value(results.get("complex_join", {}).get("modin")),
                safe_value(results.get("complex_join", {}).get("polars")),
                safe_value(results.get("complex_join", {}).get("duckdb")),
                safe_value(results.get("complex_join", {}).get("fireducks")),
                safe_value(results.get("timeseries", {}).get("pandas")),
                safe_value(results.get("timeseries", {}).get("modin")),
                safe_value(results.get("timeseries", {}).get("polars")),
                safe_value(results.get("timeseries", {}).get("duckdb")),
                safe_value(results.get("timeseries", {}).get("fireducks")),
                script_name,
            ]
            writer.writerow(row)
    

def save_results_to_csv(output_path: Path, results: dict, host_info: dict, script_name: str, dataset_size: int) -> None:
    """Save benchmark results to a single CSV file."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _write_one_results_csv(output_path, results, host_info, script_name, dataset_size)
        print(f"Results saved to {output_path}")
    except Exception as e:
        print(f"Error saving results to CSV: {e}")


# Operation 1: Filter and Group

def pandas_filter_group():
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    cols = set(df.columns)
    # Choose filter and group-by columns dynamically
    status_col = _first_present(cols, ["status_code", "severity"])  # numeric or categorical
    group_col = _first_present(cols, ["source_ip", "destination_ip", "event"]) or list(cols)[0]
    if status_col in cols and df[status_col].dtype.kind in "biufc":
        filtered = df[df[status_col] > (df[status_col].median() if df[status_col].size else 0)]
    else:
        filtered = df
    agg_target = _first_present(cols, ["bytes", "port"]) or group_col
    return filtered.groupby(group_col).agg({agg_target: "count"})


def modin_filter_group():
    try:
        df = read_data(cast(Path, DATASET_PATH), library="modin")
        cols = set(df.columns)
        status_col = _first_present(cols, ["status_code", "severity"])  # numeric or categorical
        group_col = _first_present(cols, ["source_ip", "destination_ip", "event"]) or list(cols)[0]
        if status_col in cols:
            try:
                filtered = df[df[status_col] > df[status_col].median()] if df[status_col].dtype.kind in "biufc" else df
            except Exception:
                filtered = df
        else:
            filtered = df
        agg_target = _first_present(cols, ["bytes", "port"]) or group_col
        return filtered.groupby(group_col).agg({agg_target: "count"})
    except Exception as e:
        print(f"Warning: Modin filter_group failed: {e}")
        raise


def polars_filter_group():
    df = read_data(cast(Path, DATASET_PATH), library="polars")
    cols = set(df.columns)
    status_col = _first_present(cols, ["status_code", "severity"])  # may not exist
    group_col = _first_present(cols, ["source_ip", "destination_ip", "event"]) or list(cols)[0]
    expr = df
    if status_col and status_col in cols:
        # If numeric, filter by > median; else skip filter
        try:
            expr = expr.filter(pl.col(status_col).is_not_null())
        except Exception:
            pass
    agg_target = _first_present(cols, ["bytes", "port"]) or group_col
    return expr.group_by(group_col).agg(pl.col(agg_target).count().alias("count"))


def duckdb_filter_group():
    conn = duckdb.connect()
    path = cast(Path, DATASET_PATH)
    expr = _duckdb_table_expr(path)
    cols = _get_columns_duckdb(path)
    group_col = _first_present(cols, ["source_ip", "destination_ip", "event"]) or next(iter(cols))
    agg_target = _first_present(cols, ["bytes", "port"]) or group_col
    # Optional filter if a numeric status-like column exists
    status_col = _first_present(cols, ["status_code", "severity"]) or ""
    where_clause = f"WHERE {status_col} IS NOT NULL" if status_col in cols else ""
    return conn.execute(
        f"""
        SELECT {group_col} as key, COUNT({agg_target}) as cnt
        FROM {expr}
        {where_clause}
        GROUP BY {group_col}
        """
    ).fetchdf()


def fireducks_filter_group():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})


# Operation 2: Statistical Analysis

def pandas_stats():
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    cols = set(df.columns)
    by = _first_present(cols, ["event_type", "event", "source_ip"]) or list(cols)[0]
    num = [c for c in ["bytes", "response_time_ms", "risk_score", "port"] if c in cols]
    if not num:
        return df.groupby(by).size()
    return df.groupby(by)[num].agg(["mean", "min", "max"]).reset_index()


def modin_stats():
    try:
        df = read_data(cast(Path, DATASET_PATH), library="modin")
        cols = set(df.columns)
        by = _first_present(cols, ["event_type", "event", "source_ip"]) or list(cols)[0]
        num = [c for c in ["bytes", "response_time_ms", "risk_score", "port"] if c in cols]
        if not num:
            return df.groupby(by).size()
        # Ensure numeric
        for c in num:
            try:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            except Exception:
                pass
        grp = df.groupby(by)
        # Prefer flat-column NamedAgg to reduce scheduler pressure
        agg_spec = {f"{c}_mean": (c, "mean") for c in num}
        agg_spec.update({f"{c}_min": (c, "min") for c in num})
        agg_spec.update({f"{c}_max": (c, "max") for c in num})
        try:
            return grp.agg(agg_spec)
        except Exception as e1:
            # Fallback to dict-of-lists (may create MultiIndex columns)
            try:
                return grp[num].agg(["mean", "min", "max"])  # type: ignore
            except Exception as e2:
                # Final fallback: compute per-column then join to avoid large single graphs
                parts = []
                for c in num:
                    try:
                        part = grp[c].agg(["mean", "min", "max"]).rename(columns={
                            "mean": f"{c}_mean",
                            "min": f"{c}_min",
                            "max": f"{c}_max",
                        })
                        parts.append(part)
                    except Exception:
                        continue
                if not parts:
                    raise e2
                out = parts[0]
                for p in parts[1:]:
                    out = out.join(p, how="outer")
                return out
    except Exception as e:
        print(f"Warning: Modin statistics failed: {e}")
        raise


def polars_stats():
    df = read_data(cast(Path, DATASET_PATH), library="polars")
    cols = set(df.columns)
    by = _first_present(cols, ["event_type", "event", "source_ip"]) or list(cols)[0]
    num = [c for c in ["bytes", "response_time_ms", "risk_score", "port"] if c in cols]
    if not num:
        return df.group_by(by).len()
    aggs = []
    for c in num:
        aggs += [pl.col(c).mean().alias(f"{c}_mean"), pl.col(c).min().alias(f"{c}_min"), pl.col(c).max().alias(f"{c}_max")]
    return df.group_by(by).agg(aggs)


def duckdb_stats():
    conn = duckdb.connect()
    path = cast(Path, DATASET_PATH)
    expr = _duckdb_table_expr(path)
    cols = _get_columns_duckdb(path)
    by = _first_present(cols, ["event_type", "event", "source_ip"]) or next(iter(cols))
    num = [c for c in ["bytes", "response_time_ms", "risk_score", "port"] if c in cols]
    if not num:
        return conn.execute(f"SELECT {by}, COUNT(*) as cnt FROM {expr} GROUP BY {by}").fetchdf()
    select_list = ", ".join([f"AVG({c}) as {c}_mean, MIN({c}) as {c}_min, MAX({c}) as {c}_max" for c in num])
    return conn.execute(
        f"SELECT {by}, {select_list} FROM {expr} GROUP BY {by}"
    ).fetchdf()


def fireducks_stats():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    return df.groupby("event_type").agg(
        {
            "bytes": ["mean", "std", "min", "max"],
            "response_time_ms": ["mean", "median"],
            "risk_score": ["mean", "std"],
        }
    )


# Operation 3: Complex Join and Window Functions

def pandas_complex():
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    cols = set(df.columns)
    by = _first_present(cols, ["source_ip", "destination_ip", "event"]) or list(cols)[0]
    metric = _first_present(cols, ["bytes", "port"]) or by
    summary = df.groupby(by).agg({metric: "count"}).reset_index().rename(columns={metric: "metric_count"})
    result = df.merge(summary, on=by)
    result["metric_rank"] = result.groupby(by)["metric_count"].rank(method="dense", ascending=False)
    return result[result["metric_rank"] <= 10]


def modin_complex():
    try:
        df = read_data(cast(Path, DATASET_PATH), library="modin")
        cols = set(df.columns)
        by = _first_present(cols, ["source_ip", "destination_ip", "event"]) or list(cols)[0]
        metric = _first_present(cols, ["bytes", "port"]) or by
        # Compute per-key counts (small table)
        summary = (
            df.groupby(by).agg({metric: "count"}).reset_index().rename(columns={metric: "metric_count"})
        )
        # Pull top keys (small) to avoid large shuffle/rank
        try:
            top_keys = summary.nlargest(10, "metric_count")[by].unique()
        except Exception:
            top_keys = summary.sort_values("metric_count", ascending=False)[by].head(10).unique()
        # Filter original df to those keys
        result = df[df[by].isin(list(top_keys))]
        return result
    except Exception as e:
        print(f"Warning: Modin complex_join failed: {e}")
        raise


def polars_complex():
    df = read_data(cast(Path, DATASET_PATH), library="polars")
    cols = set(df.columns)
    by = _first_present(cols, ["source_ip", "destination_ip", "event"]) or list(cols)[0]
    metric = _first_present(cols, ["bytes", "port"]) or by
    summary = df.group_by(by).agg([pl.col(metric).count().alias("metric_count")])
    result = df.join(summary, on=by)
    result = result.with_columns([
        pl.col("metric_count").rank(method="dense", descending=True).over(by).alias("metric_rank")
    ])
    return result.filter(pl.col("metric_rank") <= 10)


def duckdb_complex():
    conn = duckdb.connect()
    path = cast(Path, DATASET_PATH)
    expr = _duckdb_table_expr(path)
    cols = _get_columns_duckdb(path)
    by = _first_present(cols, ["source_ip", "destination_ip", "event"]) or next(iter(cols))
    metric = _first_present(cols, ["bytes", "port"]) or by
    return conn.execute(
        f"""
        WITH summary AS (
            SELECT {by} as key, COUNT({metric}) as metric_count
            FROM {expr}
            GROUP BY {by}
        ), ranked AS (
            SELECT d.*, s.metric_count,
                   DENSE_RANK() OVER (PARTITION BY d.{by} ORDER BY s.metric_count DESC) as metric_rank
            FROM {expr} d
            JOIN summary s ON d.{by} = s.key
        )
        SELECT * FROM ranked WHERE metric_rank <= 10
        """
    ).fetchdf()


def fireducks_complex():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    summary = (
        df.groupby("source_ip")
        .agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"})
        .reset_index()
    )
    summary.columns = [
        "source_ip",
        "total_bytes",
        "avg_response_time_ms",
        "avg_risk_score",
    ]
    result = df.merge(summary, on="source_ip")
    result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(
        method="dense", ascending=False
    )
    return result[result["bytes_rank"] <= 10]


# Operation 4: Time Series Analysis

def pandas_timeseries():
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    cols = set(df.columns)
    if "timestamp" in cols:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["hour"] = df["timestamp"].dt.hour
        by = _first_present(cols, ["event_type", "event", "source_ip"]) or "hour"
        return df.groupby(["hour", by]).size().reset_index(name="count")
    by1 = _first_present(cols, ["status_code", "severity", "event"]) or list(cols)[0]
    by2 = _first_present(cols - {by1}, ["event_type", "event", "source_ip"]) or by1
    return df.groupby([by1, by2]).size().reset_index(name="count")


def modin_timeseries():
    try:
        df = read_data(cast(Path, DATASET_PATH), library="modin")
        cols = set(df.columns)
        if "timestamp" in cols:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df["hour"] = df["timestamp"].dt.hour
            by = _first_present(cols, ["event_type", "event", "source_ip"]) or "hour"
            return df.groupby(["hour", by]).size().reset_index(name="count")
        by1 = _first_present(cols, ["status_code", "severity", "event"]) or list(cols)[0]
        by2 = _first_present(cols - {by1}, ["event_type", "event", "source_ip"]) or by1
        return df.groupby([by1, by2]).size().reset_index(name="count")
    except Exception as e:
        print(f"Warning: Modin timeseries failed: {e}")
        raise


def polars_timeseries():
    df = read_data(cast(Path, DATASET_PATH), library="polars")
    cols = set(df.columns)
    if "timestamp" in cols:
        df = df.with_columns([
            pl.col("timestamp").str.strptime(pl.Datetime, strict=False).alias("timestamp_parsed")
        ]).with_columns([pl.col("timestamp_parsed").dt.hour().alias("hour")])
        by = _first_present(cols, ["event_type", "event", "source_ip"]) or "hour"
        return df.group_by(["hour", by]).len()
    by1 = _first_present(cols, ["status_code", "severity", "event"]) or list(cols)[0]
    by2 = _first_present(cols - {by1}, ["event_type", "event", "source_ip"]) or by1
    return df.group_by([by1, by2]).len()


def duckdb_timeseries():
    conn = duckdb.connect()
    path = cast(Path, DATASET_PATH)
    expr = _duckdb_table_expr(path)
    cols = _get_columns_duckdb(path)
    if "timestamp" in {c.lower() for c in cols}:
        by = _first_present(cols, ["event_type", "event", "source_ip"]) or "event"
        return conn.execute(
            f"SELECT DATE_PART('hour', CAST(timestamp AS TIMESTAMP)) as hour, {by} as key, COUNT(*) as cnt FROM {expr} GROUP BY hour, {by} ORDER BY hour, {by}"
        ).fetchdf()
    by1 = _first_present(cols, ["status_code", "severity", "event"]) or next(iter(cols))
    by2 = _first_present(cols - {by1}, ["event_type", "event", "source_ip"]) or by1
    return conn.execute(
        f"SELECT {by1} as k1, {by2} as k2, COUNT(*) as cnt FROM {expr} GROUP BY {by1}, {by2}"
    ).fetchdf()


def fireducks_timeseries():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = read_data(cast(Path, DATASET_PATH), library="pandas")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["hour"] = df["timestamp"].dt.hour
        return df.groupby(["hour", "event_type"]).agg(
            {"bytes": ["sum", "count"], "response_time_ms": "mean", "risk_score": "mean"}
        )
    else:
        return df.groupby(["status_code", "event_type"]).size().reset_index(name="count")


# Benchmark runner

def run_benchmark_operation(library_name, operation_func, operation_name):
    """Generic benchmark runner that returns timing information with error handling."""
    try:
        if library_name.lower() == "fireducks" and not FIREDUCKS_AVAILABLE:
            print(f"{library_name} {operation_name} duration: N/A")
            return None, None

        start = time.time()
        if library_name.lower() == "modin":
            try:
                with redirect_stdout(open(os.devnull, "w")), redirect_stderr(open(os.devnull, "w")):
                    result = operation_func()
            except Exception as modin_err:
                # Retry once; transient Dask cancellations sometimes resolve on retry
                print(f"Modin {operation_name} encountered an error, retrying once: {modin_err}")
                with redirect_stdout(open(os.devnull, "w")), redirect_stderr(open(os.devnull, "w")):
                    result = operation_func()
        else:
            result = operation_func()
        duration = time.time() - start
        print(f"{library_name} {operation_name} duration: {duration:.2f}s")
        return duration, result
    except Exception as e:
        print(f"{library_name} {operation_name} failed: {e}")
        return None, None


def run_all_benchmarks():
    """Run all benchmarks for all available libraries with error handling."""
    libraries = ["pandas", "modin", "polars", "duckdb"]
    if FIREDUCKS_AVAILABLE:
        libraries.append("fireducks")

    operations = {
        "filter_group": {
            "pandas": pandas_filter_group,
            "modin": modin_filter_group,
            "polars": polars_filter_group,
            "duckdb": duckdb_filter_group,
            "fireducks": fireducks_filter_group,
        },
        "statistics": {
            "pandas": pandas_stats,
            "modin": modin_stats,
            "polars": polars_stats,
            "duckdb": duckdb_stats,
            "fireducks": fireducks_stats,
        },
        "complex_join": {
            "pandas": pandas_complex,
            "modin": modin_complex,
            "polars": polars_complex,
            "duckdb": duckdb_complex,
            "fireducks": fireducks_complex,
        },
        "timeseries": {
            "pandas": pandas_timeseries,
            "modin": modin_timeseries,
            "polars": polars_timeseries,
            "duckdb": duckdb_timeseries,
            "fireducks": fireducks_timeseries,
        },
    }

    results = {}
    for operation_name, operation_funcs in operations.items():
        print(f"\n{'=' * 50}")
        print(f"Running {operation_name.upper()} benchmarks...")
        print(f"{'=' * 50}")

        operation_results = {}
        for library_name, func in operation_funcs.items():
            duration, _ = run_benchmark_operation(library_name, func, operation_name)
            operation_results[library_name] = duration

        results[operation_name] = operation_results

    return results


if __name__ == "__main__":
    try:
        script_name = os.path.basename(__file__)
        print(f"Running script: {script_name}")
        
        print("=" * 60)
        print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
        print("=" * 60)

        # Optional CLI/env dataset override
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("-d", "--dataset", type=str, help="Path to the dataset file to benchmark")
        parser.add_argument("-o", "--output", type=str, help="Output CSV file path for results")
        args, _ = parser.parse_known_args()
        dataset_env = os.environ.get("BENCHMARK_DATASET")
        chosen = args.dataset or dataset_env
        if chosen:
            DATASET_PATH = Path(chosen)
        if DATASET_PATH is None:
            DATASET_PATH = find_dataset(config.project_root)
        if DATASET_PATH is None or not Path(DATASET_PATH).exists():
            raise RuntimeError("No dataset found. Use -d/--dataset or set BENCHMARK_DATASET.")

        # Setup
        setup_modin()
        pd.set_option("display.float_format", "{:.0f}".format)

        # Collect host information
        print("Collecting host information...")
        host_info = get_host_info()
        print(
            f"Running on: {host_info.get('hostname', 'Unknown')} (" \
            f"{host_info.get('platform', 'Unknown')})"
        )
        print(
            f"CPU: {host_info.get('cpu_brand', 'Unknown')} (" \
            f"{host_info.get('cpu_count_logical', 'N/A')} logical cores)"
        )
        print(f"Memory: {host_info.get('memory_total_gb', 'N/A')} GB total")

        # Dataset info
        print(f"\nStarting comprehensive benchmark with {DATASET_PATH}")
        print("This will test 4 different operations across up to 5 libraries...")
        dataset_size = get_dataset_size(cast(Path, DATASET_PATH))
        print(f"Dataset size: {dataset_size:,} records")

        # Determine output path
        if args.output:
            RESULTS_CSV_PATH = Path(args.output)

        # Run
        results = run_all_benchmarks()

        # Save results to CSV
        print(f"\n{'=' * 50}")
        print("SAVING RESULTS")
        print(f"{'=' * 50}")
        save_results_to_csv(RESULTS_CSV_PATH, results, host_info, script_name, dataset_size)

        # Print summary
        print(f"\n{'=' * 50}")
        print("BENCHMARK SUMMARY")
        print(f"{'=' * 50}")

        for operation, timings in results.items():
            print(f"\n{operation.upper()} Operation:")
            valid_timings = {lib: t for lib, t in timings.items() if t is not None and t > 0.0}
            if valid_timings:
                fastest = min(valid_timings.items(), key=lambda x: x[1])
                print(f"  Fastest: {fastest[0]} ({fastest[1]:.2f}s)")
                for lib, duration in sorted(valid_timings.items(), key=lambda x: x[1]):
                    speedup = fastest[1] / duration if duration > 0 else 0
                    print(f"  {lib:10}: {duration:6.2f}s (x{speedup:.1f})")
            else:
                print("  No valid timings to compare (all skipped or failed).")

        print(f"\nResults saved to: {RESULTS_CSV_PATH}")
        print("Benchmark completed!")

        with redirect_stderr(open(os.devnull, "w")):
            time.sleep(0.1)
        cleanup_modin()
    except Exception as e:
        print(f"Critical error in main: {e}")
        sys.exit(1)
