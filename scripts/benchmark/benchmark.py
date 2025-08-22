# benchmark.py
import time
import pandas as pd
import modin.pandas as mpd
import polars as pl
import duckdb
import platform
import modin.config as cfg
import logging
import warnings
import sys
import os
import psutil
import socket
import csv
from datetime import datetime
from contextlib import redirect_stderr

# Check if FireDucks is available (Linux/macOS only)
FIREDUCKS_AVAILABLE = False
if platform.system() in ['Linux', 'Darwin']:  # Darwin = macOS
    try:
        import fireducks.pandas as fpd
        FIREDUCKS_AVAILABLE = True
    except ImportError:
        pass

CSV_PATH = "data/synthetic_logs_test.csv"
RESULTS_CSV_PATH = "data/benchmark_results.csv"

def get_host_info():
    """Collect comprehensive host system information"""
    try:
        # Basic system info
        host_info = {
            'timestamp': datetime.now().isoformat(),
            'hostname': socket.gethostname(),
            'platform': platform.platform(),
            'system': platform.system(),
            'release': platform.release(),
            'version': platform.version(),
            'machine': platform.machine(),
            'processor': platform.processor(),

            # CPU info
            'cpu_count_logical': psutil.cpu_count(logical=True),
            'cpu_count_physical': psutil.cpu_count(logical=False),
            'cpu_freq_max': psutil.cpu_freq().max if psutil.cpu_freq() else 'N/A',
            'cpu_freq_current': psutil.cpu_freq().current if psutil.cpu_freq() else 'N/A',

            # Memory info
            'memory_total_gb': round(psutil.virtual_memory().total / (1024**3), 2),
            'memory_available_gb': round(psutil.virtual_memory().available / (1024**3), 2),

            # Python info
            'python_version': platform.python_version(),
            'python_implementation': platform.python_implementation(),
        }

        # Try to get more detailed CPU info
        try:
            import cpuinfo
            cpu_info = cpuinfo.get_cpu_info()
            host_info['cpu_brand'] = cpu_info.get('brand_raw', 'Unknown')
            host_info['cpu_arch'] = cpu_info.get('arch', 'Unknown')
        except ImportError:
            host_info['cpu_brand'] = 'Unknown (cpuinfo not available)'
            host_info['cpu_arch'] = 'Unknown'

        return host_info
    except Exception as e:
        print(f"Warning: Could not collect complete host info: {e}")
        return {
            'timestamp': datetime.now().isoformat(),
            'hostname': socket.gethostname(),
            'error': str(e)
        }

def setup_modin():
    """Initialize Modin with proper Dask configuration"""
    cfg.Engine.put("dask")
    logging.getLogger("distributed").setLevel(logging.CRITICAL)
    logging.getLogger("tornado").setLevel(logging.CRITICAL)
    warnings.filterwarnings("ignore")

def run_benchmark_operation(library_name, operation_func, operation_name):
    """Generic benchmark runner that returns timing information"""
    try:
        start = time.time()
        result = operation_func()
        duration = time.time() - start
        print(f"{library_name} {operation_name} duration: {duration:.2f}s")
        return duration, result
    except Exception as e:
        print(f"{library_name} {operation_name} failed: {e}")
        return None, None

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
    df = mpd.read_csv(CSV_PATH)
    return df.groupby("event_type").agg({
        "bytes": ["mean", "std", "min", "max"],
        "response_time_ms": ["mean", "median"],
        "risk_score": ["mean", "std"]
    })

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
        df['timestamp'] = fpd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        return df.groupby(['hour', 'event_type']).agg({
            'bytes': ['sum', 'count'],
            'response_time_ms': 'mean',
            'risk_score': 'mean'
        })
    else:
        return df.groupby(['status_code', 'event_type']).size().reset_index(name='count')

def save_results_to_csv(results, host_info):
    """Save benchmark results to CSV file"""
    # Prepare the row data
    row_data = host_info.copy()

    # Add timing results
    for operation, timings in results.items():
        for library, duration in timings.items():
            if duration is not None:
                row_data[f"{operation}_{library}_seconds"] = duration
            else:
                row_data[f"{operation}_{library}_seconds"] = "N/A"

    # Check if CSV file exists to determine if we need headers
    file_exists = os.path.exists(RESULTS_CSV_PATH)

    # Write to CSV
    with open(RESULTS_CSV_PATH, 'a', newline='', encoding='utf-8') as csvfile:
        if row_data:  # Only proceed if we have data
            fieldnames = list(row_data.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header if file is new
            if not file_exists:
                writer.writeheader()

            writer.writerow(row_data)
            print(f"Results saved to {RESULTS_CSV_PATH}")

def run_all_benchmarks():
    """Run all benchmark operations for all libraries"""

    # Define all operations
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

    for operation_name, libraries in operations.items():
        print(f"\n{'='*50}")
        print(f"Running {operation_name.upper()} benchmarks...")
        print(f"{'='*50}")

        results[operation_name] = {}

        for library_name, operation_func in libraries.items():
            print(f"\n--- {library_name.upper()} {operation_name} ---")
            duration, result = run_benchmark_operation(library_name, operation_func, operation_name)
            results[operation_name][library_name] = duration

            # Show a sample of the result (first few rows)
            if result is not None and hasattr(result, 'head'):
                try:
                    print("Sample result:")
                    print(result.head())
                except:
                    print("Result computed successfully")
            elif result is not None:
                print("Result computed successfully")

    return results

if __name__ == "__main__":
    print("="*60)
    print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
    print("="*60)

    # Setup
    setup_modin()
    pd.set_option('display.float_format', '{:.0f}'.format)

    # Collect host information
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
    save_results_to_csv(results, host_info)

    # Print summary
    print(f"\n{'='*50}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*50}")

    for operation, timings in results.items():
        print(f"\n{operation.upper()} Operation:")
        valid_timings = {lib: time for lib, time in timings.items() if time is not None}
        if valid_timings:
            fastest = min(valid_timings.items(), key=lambda x: x[1])
            print(f"  Fastest: {fastest[0]} ({fastest[1]:.2f}s)")
            for lib, duration in sorted(valid_timings.items(), key=lambda x: x[1]):
                speedup = fastest[1] / duration if duration > 0 else 0
                print(f"  {lib:10}: {duration:6.2f}s (x{speedup:.1f})")

    print(f"\nResults saved to: {RESULTS_CSV_PATH}")
    print("Benchmark completed!")

    # Suppress any remaining output
    with redirect_stderr(open(os.devnull, 'w')):
        time.sleep(0.1)  # Brief pause for cleanup
