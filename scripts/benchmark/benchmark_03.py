# benchmark_03.py

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
import gc
from datetime import datetime
from contextlib import redirect_stderr
from dask.distributed import Client, LocalCluster

# --- Library Availability ---
FIREDUCKS_AVAILABLE = False
if platform.system() in ['Linux', 'Darwin']:
    try:
        import fireducks.pandas as fpd
        FIREDUCKS_AVAILABLE = True
    except ImportError:
        pass

CSV_PATH = "data/synthetic_logs_test.csv"
RESULTS_CSV_PATH = "data/benchmark_results.csv"
MAX_MEMORY_GB = 8  # Hard memory limit

def get_host_info():
    """Collect comprehensive host system information"""
    try:
        host_info = {
            'timestamp': datetime.now().isoformat(),
            'hostname': socket.gethostname(),
            'platform': platform.platform(),
            'system': platform.system(),
            'release': platform.release(),
            'version': platform.version(),
            'machine': platform.machine(),
            'processor': platform.processor(),
            'cpu_count_logical': psutil.cpu_count(logical=True),
            'cpu_count_physical': psutil.cpu_count(logical=False),
            'cpu_freq_max': psutil.cpu_freq().max if psutil.cpu_freq() else 'N/A',
            'cpu_freq_current': psutil.cpu_freq().current if psutil.cpu_freq() else 'N/A',
            'memory_total_gb': round(psutil.virtual_memory().total / (1024**3), 2),
            'memory_available_gb': round(psutil.virtual_memory().available / (1024**3), 2),
            'python_version': platform.python_version(),
            'python_implementation': platform.python_implementation(),
        }
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

def get_memory_usage_mb():
    """Get memory usage of the current process and its children"""
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss
    for child in process.children(recursive=True):
        try:
            mem += child.memory_info().rss
        except:
            pass
    return mem / (1024 ** 2)

def log_memory_usage(label=""):
    """Log current memory usage (including child processes)"""
    mem_usage_mb = get_memory_usage_mb()
    print(f"[{label}] Memory usage: {mem_usage_mb:.2f} MB")

def check_memory_limit():
    """Check if memory usage exceeds the hard limit"""
    mem_usage_gb = get_memory_usage_mb() / 1024
    if mem_usage_gb > MAX_MEMORY_GB:
        raise MemoryError(f"Memory limit exceeded: {mem_usage_gb:.2f} GB > {MAX_MEMORY_GB} GB")

def setup_modin():
    """Initialize Modin with conservative Dask configuration for Windows"""
    cfg.Engine.put("dask")
    cfg.NPartitions.put(2)  # Reduce partitions for stability on Windows
    import dask
    dask.config.set({
        "distributed.worker.memory.target": 0.6,  # 60% of total memory
        "distributed.worker.memory.spill": 0.8,   # Spill to disk if needed
        "distributed.worker.memory.pause": 0.7,   # Pause at 70%
        "distributed.worker.memory.terminate": 0.8,  # Terminate at 80%
        "distributed.scheduler.worker-saturation": 0.8,  # Allow moderate CPU usage
    })
    logging.getLogger("distributed").setLevel(logging.CRITICAL)
    warnings.filterwarnings("ignore")

def run_modin_operation(func, csv_path):
    """Run a Modin operation with a conservative Dask cluster for Windows"""
    cluster = LocalCluster(
        n_workers=2,  # Reduce workers for stability on Windows
        threads_per_worker=1,  # Single-threaded workers
        memory_limit="4GB",  # Lower memory limit
        silence_logs=logging.CRITICAL
    )
    client = Client(cluster)
    try:
        log_memory_usage(f"Modin {func.__name__} (start)")
        result = func(csv_path)
        log_memory_usage(f"Modin {func.__name__} (end)")
        return result
    finally:
        client.close()
        cluster.close()
        gc.collect()

def run_benchmark_operation(library_name, operation_func, operation_name, csv_path):
    """Generic benchmark runner with memory logging and garbage collection"""
    try:
        log_memory_usage(f"{library_name} {operation_name} (start)")
        check_memory_limit()
        start = time.time()
        result = operation_func(csv_path)
        duration = time.time() - start
        print(f"{library_name} {operation_name} duration: {duration:.2f}s")
        log_memory_usage(f"{library_name} {operation_name} (end)")
        del result  # Explicitly delete the result
        gc.collect()  # Force garbage collection
        return duration
    except Exception as e:
        print(f"{library_name} {operation_name} failed: {e}")
        log_memory_usage(f"{library_name} {operation_name} (failed)")
        return None

# --- Pandas Operations ---
def pandas_filter_group(csv_path):
    df = pd.read_csv(csv_path)
    result = df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})
    return result

def pandas_stats(csv_path):
    df = pd.read_csv(csv_path)
    result = df.groupby("event_type").agg({
        "bytes": ["mean", "std", "min", "max"],
        "response_time_ms": ["mean", "median"],
        "risk_score": ["mean", "std"]
    })
    return result

def pandas_complex(csv_path):
    df = pd.read_csv(csv_path)
    summary = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"}).reset_index()
    summary.columns = ["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"]
    result = df.merge(summary, on="source_ip")
    result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
    return result[result["bytes_rank"] <= 10]

def pandas_timeseries(csv_path):
    df = pd.read_csv(csv_path)
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

# --- Modin Operations (with fallback to Pandas) ---
def modin_filter_group(csv_path):
    try:
        def _modin_filter_group(path):
            df = mpd.read_csv(path)
            return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})
        return run_modin_operation(_modin_filter_group, csv_path)
    except Exception as e:
        print(f"Modin failed, falling back to Pandas: {e}")
        return pandas_filter_group(csv_path)

def modin_stats(csv_path):
    try:
        def _modin_stats(path):
            df = mpd.read_csv(path)
            return df.groupby("event_type").agg({
                "bytes": ["mean", "std", "min", "max"],
                "response_time_ms": ["mean", "median"],
                "risk_score": ["mean", "std"]
            })
        return run_modin_operation(_modin_stats, csv_path)
    except Exception as e:
        print(f"Modin failed, falling back to Pandas: {e}")
        return pandas_stats(csv_path)

def modin_complex(csv_path):
    try:
        def _modin_complex(path):
            df = mpd.read_csv(path)
            summary = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"}).reset_index()
            summary.columns = ["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"]
            result = df.merge(summary, on="source_ip")
            result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
            return result[result["bytes_rank"] <= 10]
        return run_modin_operation(_modin_complex, csv_path)
    except Exception as e:
        print(f"Modin failed, falling back to Pandas: {e}")
        return pandas_complex(csv_path)

def modin_timeseries(csv_path):
    try:
        def _modin_timeseries(path):
            df = mpd.read_csv(path)
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
        return run_modin_operation(_modin_timeseries, csv_path)
    except Exception as e:
        print(f"Modin failed, falling back to Pandas: {e}")
        return pandas_timeseries(csv_path)

# --- Polars Operations (Optimized for Memory) ---
def polars_filter_group(csv_path):
    # Use streaming and chunking
    df = pl.scan_csv(csv_path)
    result = (df.filter(pl.col("status_code") == 200)
               .group_by("source_ip")
               .agg(pl.sum("bytes")))
    return result.collect(streaming=True)  # Force streaming

def polars_stats(csv_path):
    df = pl.scan_csv(csv_path)
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
    return result.collect(streaming=True)

def polars_complex(csv_path):
    df = pl.scan_csv(csv_path)
    summary = (df.group_by("source_ip")
                .agg([pl.col("bytes").sum().alias("total_bytes"),
                      pl.col("response_time_ms").mean().alias("avg_response_time_ms"),
                      pl.col("risk_score").mean().alias("avg_risk_score")]))
    result = (df.join(summary, on="source_ip")
               .with_columns([
                   pl.col("bytes").rank(method="dense", descending=True).over("event_type").alias("bytes_rank")
               ])
               .filter(pl.col("bytes_rank") <= 10))
    return result.collect(streaming=True)

def polars_timeseries(csv_path):
    df = pl.scan_csv(csv_path)
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
    return result.collect(streaming=True)

# --- DuckDB Operations ---
def duckdb_filter_group(csv_path):
    with duckdb.connect() as conn:
        return conn.execute(f"""
            SELECT source_ip, SUM(bytes) as bytes
            FROM read_csv_auto('{csv_path}')
            WHERE status_code = 200
            GROUP BY source_ip
        """).fetchdf()

def duckdb_stats(csv_path):
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
            FROM read_csv_auto('{csv_path}')
            GROUP BY event_type
        """).fetchdf()

def duckdb_complex(csv_path):
    with duckdb.connect() as conn:
        return conn.execute(f"""
            WITH summary AS (
                SELECT source_ip,
                       SUM(bytes) as total_bytes,
                       AVG(response_time_ms) as avg_response_time_ms,
                       AVG(risk_score) as avg_risk_score
                FROM read_csv_auto('{csv_path}')
                GROUP BY source_ip
            ),
            ranked AS (
                SELECT d.*, s.total_bytes, s.avg_response_time_ms, s.avg_risk_score,
                       DENSE_RANK() OVER (PARTITION BY d.event_type ORDER BY d.bytes DESC) as bytes_rank
                FROM read_csv_auto('{csv_path}') d
                JOIN summary s ON d.source_ip = s.source_ip
            )
            SELECT * FROM ranked WHERE bytes_rank <= 10
        """).fetchdf()

def duckdb_timeseries(csv_path):
    with duckdb.connect() as conn:
        try:
            return conn.execute(f"""
                SELECT EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) as hour,
                       event_type,
                       SUM(bytes) as bytes_sum,
                       COUNT(bytes) as bytes_count,
                       AVG(response_time_ms) as response_time_ms_mean,
                       AVG(risk_score) as risk_score_mean
                FROM read_csv_auto('{csv_path}')
                GROUP BY hour, event_type
                ORDER BY hour, event_type
            """).fetchdf()
        except:
            return conn.execute(f"""
                SELECT status_code, event_type, COUNT(*) as count
                FROM read_csv_auto('{csv_path}')
                GROUP BY status_code, event_type
            """).fetchdf()

# --- FireDucks Operations ---
def fireducks_filter_group(csv_path):
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(csv_path)
    return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})

def fireducks_stats(csv_path):
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(csv_path)
    return df.groupby("event_type").agg({
        "bytes": ["mean", "std", "min", "max"],
        "response_time_ms": ["mean", "median"],
        "risk_score": ["mean", "std"]
    })

def fireducks_complex(csv_path):
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(csv_path)
    summary = df.groupby("source_ip").agg({"bytes": "sum", "response_time_ms": "mean", "risk_score": "mean"}).reset_index()
    summary.columns = ["source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"]
    result = df.merge(summary, on="source_ip")
    result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(method="dense", ascending=False)
    return result[result["bytes_rank"] <= 10]

def fireducks_timeseries(csv_path):
    if not FIREDUCKS_AVAILABLE:
        return None
    df = fpd.read_csv(csv_path)
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

# --- Benchmark Runner ---
def run_library_benchmarks(library_name, csv_path):
    """Run all operations for a single library"""
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
        if duration is not None and duration > 0:
            results[operation_name] = duration
    return results

def run_all_benchmarks(csv_path):
    """Run all benchmarks for all available libraries, one library at a time"""
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

def save_results_to_csv(results, host_info):
    """Save benchmark results to CSV file"""
    row_data = host_info.copy()
    for library, timings in results.items():
        for operation, duration in timings.items():
            row_data[f"{operation}_{library}_seconds"] = duration
    file_exists = os.path.exists(RESULTS_CSV_PATH)
    with open(RESULTS_CSV_PATH, 'a', newline='', encoding='utf-8') as csvfile:
        if row_data:
            fieldnames = list(row_data.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row_data)
            print(f"Results saved to {RESULTS_CSV_PATH}")

if __name__ == "__main__":
    print("="*60)
    print("COMPREHENSIVE DATA PROCESSING BENCHMARK")
    print("="*60)
    setup_modin()
    pd.set_option('display.float_format', '{:.0f}'.format)
    host_info = get_host_info()
    print(f"Running on: {host_info.get('hostname', 'Unknown')} ({host_info.get('platform', 'Unknown')})")
    print(f"CPU: {host_info.get('cpu_brand', 'Unknown')} ({host_info.get('cpu_count_logical', 'N/A')} logical cores)")
    print(f"Memory: {host_info.get('memory_total_gb', 'N/A')} GB total")
    print(f"\nStarting comprehensive benchmark with {CSV_PATH}")
    print("This will test 4 different operations across all available libraries...")
    log_memory_usage("Initial memory usage")
    results = run_all_benchmarks(CSV_PATH)
    log_memory_usage("Final memory usage")
    save_results_to_csv(results, host_info)
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
    print(f"\nResults saved to: {RESULTS_CSV_PATH}")
    print("Benchmark completed!")
