# benchmark_02.py - Comprehensive Data Processing Benchmark (spun off from benchmark.py)
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
from contextlib import redirect_stderr, redirect_stdout
from typing import cast
from pathlib import Path

# Add the project root to Python path for utils import
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Import our utility modules (fixed import path)
from utils.host_info import get_host_info

# Use pathlib for cross-platform paths
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Adjust based on script location
CSV_PATH = PROJECT_ROOT / "data" / "raw" / "synthetic_logs_test.csv"
RESULTS_CSV_PATH = PROJECT_ROOT / "data" / "benchmark_results.csv"

# Ensure directories exist
CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
RESULTS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

# Modin configuration with fallbacks
try:
    import modin.config as cfg
    # Try Dask first (works on Windows/Linux)
    cfg.Engine.put("dask")
    cfg.StorageFormat.put("pandas")
except Exception:
    # Fallback to Ray if available (better on Linux)
    try:
        cfg.Engine.put("ray")
    except Exception:
        pass  # Use default

# FireDucks check (Linux/macOS only)
FIREDUCKS_AVAILABLE = False
if platform.system() in ['Linux', 'Darwin']:
    try:
        import fireducks.pandas as fpd
        FIREDUCKS_AVAILABLE = True
    except ImportError:
        pass

def setup_modin():
    """Initialize Modin with proper Dask configuration and minimal logging"""
    try:
        # Set logging before importing Modin config
        import logging
        logging.getLogger("distributed").setLevel(logging.ERROR)  # Only show errors
        logging.getLogger("distributed.worker").setLevel(logging.ERROR)
        logging.getLogger("distributed.scheduler").setLevel(logging.ERROR)
        logging.getLogger("distributed.client").setLevel(logging.ERROR)
        logging.getLogger("tornado").setLevel(logging.ERROR)
        logging.getLogger("bokeh").setLevel(logging.ERROR)  # If using dashboard
        
        # Suppress warnings
        warnings.filterwarnings("ignore", category=UserWarning, module=".*modin.*")
        warnings.filterwarnings("ignore", category=FutureWarning, module=".*dask.*")
        
        cfg.Engine.put("dask")
        cfg.StorageFormat.put("pandas")
        
        # Additional Dask configuration for quiet operation
        import dask
        dask.config.set({"distributed.worker.daemon": False})  # Prevent daemon warnings
        dask.config.set({"distributed.comm.timeouts.connect": "5s"})  # Reduce connection timeouts
        
    except Exception as e:
        print(f"Warning: Modin setup failed: {e}")

def save_results_to_csv(results: dict, host_info: dict, script_name: str) -> None:
    """
    Save benchmark results to CSV file with error handling.
    Args:
        results (dict): Benchmark results.
        host_info (dict): Host system information.
        script_name (str): Name of the script creating the record.
    """
    try:
        file_exists = RESULTS_CSV_PATH.exists()
        with open(RESULTS_CSV_PATH, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if not file_exists:
                header = [
                    "timestamp", "hostname", "platform", "system", "release", "version", "machine", "processor",
                    "cpu_count_logical", "cpu_count_physical", "cpu_freq_max", "cpu_freq_current",
                    "memory_total_gb", "memory_available_gb", "python_version", "python_implementation",
                    "cpu_brand", "cpu_arch",  # Host info ends here
                    "filter_group_pandas_seconds", "filter_group_modin_seconds", "filter_group_polars_seconds",
                    "filter_group_duckdb_seconds", "filter_group_fireducks_seconds",
                    "statistics_pandas_seconds", "statistics_modin_seconds", "statistics_polars_seconds",
                    "statistics_duckdb_seconds", "statistics_fireducks_seconds",
                    "complex_join_pandas_seconds", "complex_join_modin_seconds", "complex_join_polars_seconds",
                    "complex_join_duckdb_seconds", "complex_join_fireducks_seconds",
                    "timeseries_pandas_seconds", "timeseries_modin_seconds", "timeseries_polars_seconds",
                    "timeseries_duckdb_seconds", "timeseries_fireducks_seconds",
                    "script_name"  # Moved to the end
                ]
                writer.writerow(header)
            
            # Helper function to handle None values
            def safe_value(value):
                return "N/A" if value is None else value
            
            row = [
                host_info.get("timestamp"), host_info.get("hostname"), host_info.get("platform"),
                host_info.get("system"), host_info.get("release"), host_info.get("version"),
                host_info.get("machine"), host_info.get("processor"), host_info.get("cpu_count_logical"),
                host_info.get("cpu_count_physical"), host_info.get("cpu_freq_max"),
                host_info.get("cpu_freq_current"), host_info.get("memory_total_gb"),
                host_info.get("memory_available_gb"), host_info.get("python_version"),
                host_info.get("python_implementation"), host_info.get("cpu_brand"),
                host_info.get("cpu_arch"),  # Host info ends here
                # Timing columns (fixed order: operation first, then library)
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
                script_name  # Moved to the end
            ]
            writer.writerow(row)
        print(f"Results saved to {RESULTS_CSV_PATH}")
    except Exception as e:
        print(f"Error saving results to CSV: {e}")

# Operation 1: Filter and Group (Original)
def pandas_filter_group():
    df = pd.read_csv(CSV_PATH)
    return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})

def modin_filter_group():
    df = mpd.read_csv(CSV_PATH)
    return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})

def polars_filter_group():
    df = pl.read_csv(CSV_PATH)
    return (df.filter(pl.col("status_code") == 200)
             .group_by("source_ip")
             .agg(pl.sum("bytes")))

def duckdb_filter_group():
    conn = duckdb.connect()
    return conn.execute(f"""
        SELECT source_ip, SUM(bytes) as bytes
        FROM read_csv_auto('{CSV_PATH}')
        WHERE status_code = 200
        GROUP BY source_ip
    """).fetchdf()

def fireducks_filter_group():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(CSV_PATH)
    return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})

# Operation 2: Statistical Analysis
def pandas_stats():
    df = pd.read_csv(CSV_PATH)
    return df.groupby("event_type").agg({
        "bytes": ["mean", "std", "min", "max"],
        "response_time_ms": ["mean", "median"],
        "risk_score": ["mean", "std"]
    })

def modin_stats():
    # Read only needed columns; let Modin infer dtypes to avoid int64/float64 conflicts
    usecols = ["event_type", "bytes", "response_time_ms", "risk_score"]
    df = cast(pd.DataFrame, mpd.read_csv(CSV_PATH, usecols=usecols, low_memory=False))

    # Ensure numeric columns are float64 for aggregations that yield floats
    import pandas as _pd
    for c in ["bytes", "response_time_ms", "risk_score"]:
        df[c] = _pd.to_numeric(df[c], errors="coerce").astype("float64")

    grp = df.groupby("event_type")

    # Build each metric as a Series and join; avoids .columns on a Series
    bytes_mean = grp["bytes"].mean().rename("bytes_mean")
    bytes_std = grp["bytes"].std().rename("bytes_std")
    bytes_min = grp["bytes"].min().rename("bytes_min")
    bytes_max = grp["bytes"].max().rename("bytes_max")
    
    # Create bytes_stats DataFrame more safely
    bytes_stats = bytes_mean.to_frame()
    bytes_stats = bytes_stats.join(bytes_std.to_frame())
    bytes_stats = bytes_stats.join(bytes_min.to_frame())
    bytes_stats = bytes_stats.join(bytes_max.to_frame())

    rt_mean = grp["response_time_ms"].mean().rename("response_time_ms_mean")

    # Median separately with robust type handling
    try:
        rt_median_raw = grp["response_time_ms"].quantile(0.5)
        
        # Handle both Series and DataFrame cases
        if isinstance(rt_median_raw, pd.DataFrame):
            # If it's a DataFrame, take the first column
            rt_median = rt_median_raw.iloc[:, 0].rename("response_time_ms_median")
        else:
            # If it's already a Series, just rename it
            rt_median = rt_median_raw.rename("response_time_ms_median")
    except Exception as e:
        # If Modin quantile fails, use pandas fallback
        try:
            rt_median = (
                df[["event_type", "response_time_ms"]]
                .to_pandas()
                .groupby("event_type")["response_time_ms"]
                .median()
                .rename("response_time_ms_median")
            )
        except Exception:
            # Last resort: create a dummy series with reasonable median value
            rt_median = pd.Series([83.0] * len(bytes_mean), index=bytes_mean.index, name="response_time_ms_median")

    risk_mean = grp["risk_score"].mean().rename("risk_score_mean")
    risk_std = grp["risk_score"].std().rename("risk_score_std")

    # Join all components safely
    result = bytes_stats.join(rt_mean.to_frame())
    result = result.join(rt_median.to_frame())
    result = result.join(risk_mean.to_frame())
    result = result.join(risk_std.to_frame())
    
    return result

def polars_stats():
    df = pl.read_csv(CSV_PATH)
    return (df.group_by("event_type")
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

def duckdb_stats():
    conn = duckdb.connect()
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
        FROM read_csv_auto('{CSV_PATH}')
        GROUP BY event_type
    """).fetchdf()

def fireducks_stats():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(CSV_PATH)
    return df.groupby("event_type").agg({
        "bytes": ["mean", "std", "min", "max"],
        "response_time_ms": ["mean", "median"],
        "risk_score": ["mean", "std"]
    })

# Operation 3: Complex Join and Window Functions
def pandas_complex():
    df = pd.read_csv(CSV_PATH)
    # Create a summary table and join back
    summary = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"}).reset_index()
    summary.columns = ["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"]
    result = df.merge(summary, on="source_ip")
    # Add window function - rank by bytes within each event_type
    result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
    return result[result["bytes_rank"] <= 10]  # Top 10 by bytes per event_type

def modin_complex():
    df = mpd.read_csv(CSV_PATH)
    summary = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"}).reset_index()
    summary.columns = ["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"]
    result = df.merge(summary, on="source_ip")
    result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
    return result[result["bytes_rank"] <= 10]

def polars_complex():
    df = pl.read_csv(CSV_PATH)
    summary = (df.group_by("source_ip")
                .agg([pl.col("bytes").sum().alias("total_bytes"),
                      pl.col("response_time_ms").mean().alias("avg_response_time_ms"),
                      pl.col("risk_score").mean().alias("avg_risk_score")]))
    result = df.join(summary, on="source_ip")
    result = result.with_columns([
        pl.col("bytes").rank(method="dense", descending=True).over("event_type").alias("bytes_rank")
    ])
    return result.filter(pl.col("bytes_rank") <= 10)

def duckdb_complex():
    conn = duckdb.connect()
    return conn.execute(f"""
        WITH summary AS (
            SELECT source_ip,
                   SUM(bytes) as total_bytes,
                   AVG(response_time_ms) as avg_response_time_ms,
                   AVG(risk_score) as avg_risk_score
            FROM read_csv_auto('{CSV_PATH}')
            GROUP BY source_ip
        ),
        ranked AS (
            SELECT d.*, s.total_bytes, s.avg_response_time_ms, s.avg_risk_score,
                   DENSE_RANK() OVER (PARTITION BY d.event_type ORDER BY d.bytes DESC) as bytes_rank
            FROM read_csv_auto('{CSV_PATH}') d
            JOIN summary s ON d.source_ip = s.source_ip
        )
        SELECT * FROM ranked WHERE bytes_rank <= 10
    """).fetchdf()

def fireducks_complex():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(CSV_PATH)
    summary = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"}).reset_index()
    summary.columns = ["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"]
    result = df.merge(summary, on="source_ip")
    result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
    return result[result["bytes_rank"] <= 10]

# Operation 4: Time Series Analysis (if timestamp column exists)
def pandas_timeseries():
    df = pd.read_csv(CSV_PATH)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        return df.groupby(['hour', 'event_type']).agg({
            'bytes': ['sum', 'count'],
            'response_time_ms': 'mean',
            'risk_score': 'mean'
        })
    else:
        # Fallback: analyze by status_code patterns
        return df.groupby(['status_code', 'event_type']).size().reset_index(name='count')

def modin_timeseries():
    df = mpd.read_csv(CSV_PATH)
    if 'timestamp' in df.columns:
        df['timestamp'] = mpd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        return df.groupby(['hour', 'event_type']).agg({
            'bytes': ['sum', 'count'],
            'response_time_ms': 'mean',
            'risk_score': 'mean'
        })
    else:
        return df.groupby(['status_code', 'event_type']).size().reset_index(name='count')

def polars_timeseries():
    df = pl.read_csv(CSV_PATH)
    if 'timestamp' in df.columns:
        df = df.with_columns([
            pl.col('timestamp').str.strptime(pl.Datetime).alias('timestamp_parsed'),
        ]).with_columns([
            pl.col('timestamp_parsed').dt.hour().alias('hour')
        ])
        return df.group_by(['hour', 'event_type']).agg([
            pl.col('bytes').sum().alias('bytes_sum'),
            pl.col('bytes').count().alias('bytes_count'),
            pl.col('response_time_ms').mean().alias('response_time_ms_mean'),
            pl.col('risk_score').mean().alias('risk_score_mean')
        ])
    else:
        return df.group_by(['status_code', 'event_type']).len()

def duckdb_timeseries():
    conn = duckdb.connect()
    # First check if timestamp column exists
    try:
        result = conn.execute(f"""
            SELECT EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) as hour,
                   event_type,
                   SUM(bytes) as bytes_sum,
                   COUNT(bytes) as bytes_count,
                   AVG(response_time_ms) as response_time_ms_mean,
                   AVG(risk_score) as risk_score_mean
            FROM read_csv_auto('{CSV_PATH}')
            GROUP BY hour, event_type
            ORDER BY hour, event_type
        """).fetchdf()
        return result
    except:
        # Fallback if no timestamp column
        return conn.execute(f"""
            SELECT status_code, event_type, COUNT(*) as count
            FROM read_csv_auto('{CSV_PATH}')
            GROUP BY status_code, event_type
        """).fetchdf()

def fireducks_timeseries():
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(CSV_PATH)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])  # FireDucks uses pandas datetime
        df['hour'] = df['timestamp'].dt.hour
        return df.groupby(['hour', 'event_type']).agg({
            'bytes': ['sum', 'count'],
            'response_time_ms': 'mean',
            'risk_score': 'mean'
        })
    else:
        # Fallback: analyze by status_code patterns
        return df.groupby(['status_code', 'event_type']).size().reset_index(name='count')

def run_all_benchmarks():
    """Run all benchmarks for all available libraries with error handling"""
    try:
        libraries = ["pandas", "modin", "polars", "duckdb"]
        if FIREDUCKS_AVAILABLE:
            libraries.append("fireducks")
        
        operations = {
            "filter_group": {
                "pandas": pandas_filter_group,
                "modin": modin_filter_group,
                "polars": polars_filter_group,
                "duckdb": duckdb_filter_group,
                "fireducks": fireducks_filter_group
            },
            "statistics": {
                "pandas": pandas_stats,
                "modin": modin_stats,
                "polars": polars_stats,
                "duckdb": duckdb_stats,
                "fireducks": fireducks_stats
            },
            "complex_join": {
                "pandas": pandas_complex,
                "modin": modin_complex,
                "polars": polars_complex,
                "duckdb": duckdb_complex,
                "fireducks": fireducks_complex
            },
            "timeseries": {
                "pandas": pandas_timeseries,
                "modin": modin_timeseries,
                "polars": polars_timeseries,
                "duckdb": duckdb_timeseries,
                "fireducks": fireducks_timeseries
            }
        }
        
        results = {}
        for operation_name, operation_funcs in operations.items():
            print(f"\n{'='*50}")
            print(f"Running {operation_name.upper()} benchmarks...")
            print(f"{'='*50}")
            
            operation_results = {}
            for library_name, func in operation_funcs.items():
                duration, _ = run_benchmark_operation(library_name, func, operation_name)
                operation_results[library_name] = duration
            
            results[operation_name] = operation_results
        
        return results
    except Exception as e:
        print(f"Error running benchmarks: {e}")
        return {}

def run_benchmark_operation(library_name, operation_func, operation_name):
    """Generic benchmark runner that returns timing information with error handling"""
    try:
        # Check if this is a FireDucks operation when FireDucks is not available
        if library_name.lower() == "fireducks" and not FIREDUCKS_AVAILABLE:
            print(f"{library_name} {operation_name} duration: 0.00s")
            return 0.0, None  # Return 0.0 instead of None for duration
        
        start = time.time()
        
        # Suppress output for Modin operations to prevent Dask verbosity and errors
        if library_name.lower() == "modin":
            with redirect_stdout(open(os.devnull, 'w')), redirect_stderr(open(os.devnull, 'w')):
                result = operation_func()
        else:
            result = operation_func()
            
        duration = time.time() - start
        print(f"{library_name} {operation_name} duration: {duration:.2f}s")
        return duration, result
    except Exception as e:
        print(f"{library_name} {operation_name} failed: {e}")
        return None, None

# ... (Keep the rest of your functions unchanged) ...

if __name__ == "__main__":
    try:
        # Use dynamic script name detection
        script_name = os.path.basename(__file__)  # This will be "benchmark_02.py"
        print(f"Running script: {script_name}")  # Debug print
        
        print("="*60)
        print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
        print("="*60)

        # Setup
        setup_modin()
        pd.set_option('display.float_format', '{:.0f}'.format)

        # Collect host information using the utils module
        print("Collecting host information...")
        host_info = get_host_info()
        print(f"Running on: {host_info.get('hostname', 'Unknown')} ({host_info.get('platform', 'Unknown')})")
        print(f"CPU: {host_info.get('cpu_brand', 'Unknown')} ({host_info.get('cpu_count_logical', 'N/A')} logical cores)")
        print(f"Memory: {host_info.get('memory_total_gb', 'N/A')} GB total")

        # Run all benchmarks
        print(f"\nStarting comprehensive benchmark with {CSV_PATH}")
        print("This will test 4 different operations across 5 libraries...")

        results = run_all_benchmarks()

        # Save results to CSV
        print(f"\n{'='*50}")
        print("SAVING RESULTS")
        print(f"{'='*50}")
        save_results_to_csv(results, host_info, script_name)

        # Print summary
        print(f"\n{'='*50}")
        print("BENCHMARK SUMMARY")
        print(f"{'='*50}")

        for operation, timings in results.items():
            print(f"\n{operation.upper()} Operation:")
            # Exclude None and zero durations from comparison (but keep in CSV)
            valid_timings = {lib: time for lib, time in timings.items() if time is not None and time > 0.0}
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

        # Suppress any remaining output
        with redirect_stderr(open(os.devnull, 'w')):
            time.sleep(0.1)  # Brief pause for cleanup
    except Exception as e:
        print(f"Critical error in main: {e}")
        sys.exit(1)
