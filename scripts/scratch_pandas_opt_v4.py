"""
Scratch script to benchmark Copy-on-Write and PyArrow Strings globally in Pandas.
Compares:
1. Scenario A: Vanilla Pandas (defaults, no global optimizations)
2. Scenario B: Optimized Pandas (CoW enabled + future PyArrow string inference enabled)
"""

import gc
import sys
import time
from pathlib import Path
import psutil
import time
import gc
from dataclasses import dataclass, field
from typing import Dict, Optional
from contextlib import ContextManager
from typing_extensions import TypedDict


# Add project root to Python path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

import pandas as pd
from utils.pandas_benchmark_ops import complex_join_top_ranked, timeseries_hour_counts

from scratch_pandas_opt_utils import Timer, BenchmarkResult

CSV_DATASET_PATH = Path("data/raw/synthetic_logs_10M.csv")
PARQUET_DATASET_PATH = Path("data/raw/synthetic_logs_7M.parquet")

DATASET_PATH = PARQUET_DATASET_PATH

def read_dataset(ds: Path):
    """Read dataset from CSV or Parquet file."""
    if ds.suffix == ".csv":
        return pd.read_csv(ds)
    elif ds.suffix == ".parquet":
        return pd.read_parquet(ds, engine="auto")
    else:
        raise ValueError(f"Unsupported file format: {ds.suffix}")

def get_process_memory():
    """Return resident memory used by current process in MB."""
    process = psutil.Process()
    return process.memory_info().rss / (1024**2)

def run_benchmark_ops(df):
    """Run all benchmark operations and return durations + memory stats."""
    timings = {}
    
    # 1. Filter and Group
    gc.collect()
    start_time = time.perf_counter()
    filtered = df[df['bytes'] > 1000]
    fg_result = filtered.groupby('event_type').size().reset_index(name='count')
    timings['filter_group'] = time.perf_counter() - start_time
    del filtered, fg_result
    
    # 2. Statistical Analysis
    gc.collect()
    start_time = time.perf_counter()
    stat_cols = ['bytes', 'response_time_ms', 'risk_score']
    stats_result = df.groupby('event_type')[stat_cols].agg(['mean', 'min', 'max']).reset_index()
    timings['statistics'] = time.perf_counter() - start_time
    del stats_result
    
    # 3. Complex Join & Windowing
    gc.collect()
    start_time = time.perf_counter()
    cj_result = complex_join_top_ranked(df, rank_col="bytes_rank", observed=False, sort_by_rank=True)
    timings['complex_join'] = time.perf_counter() - start_time
    del cj_result
    
    # 4. Time Series
    gc.collect()
    start_time = time.perf_counter()
    ts_result = timeseries_hour_counts(df, observed=False, reset_index=True, hour_name="hour")
    timings['timeseries'] = time.perf_counter() - start_time
    del ts_result
    
    gc.collect()
    return timings


def benchmark_scenario_a():
    """Benchmark Vanilla Pandas."""
    print("\n--- Running Scenario A: Vanilla Pandas ---")
    gc.collect()
    time.sleep(0.5)
    
    start_mem = get_process_memory()
    start_time = time.perf_counter()
    
    # Load DataFrame normally
    df = read_dataset(PARQUET_DATASET_PATH)

    
    load_time = time.perf_counter() - start_time
    deep_mem = df.memory_usage(deep=True).sum() / (1024**2)
    process_mem_delta = get_process_memory() - start_mem
    
    print(f"  Load time: {load_time:.3f}s")
    print(f"  DataFrame Deep Memory Size: {deep_mem:.2f} MB")
    print(f"  Process Memory RSS delta: {process_mem_delta:.2f} MB")
    
    # Run operations
    timings = run_benchmark_ops(df)
    
    del df
    gc.collect()
    time.sleep(0.5)
    
    return {
        "load_time": load_time,
        "deep_mem": deep_mem,
        "process_mem_delta": process_mem_delta,
        **timings
    }


def benchmark_scenario_b():
    """Benchmark Optimized Pandas (CoW + PyArrow Strings)."""
    print("\n--- Running Scenario B: Optimized Pandas (CoW + PyArrow Strings) ---")
    
    # Enable global optimizations!
    pd.options.mode.copy_on_write = True
    pd.options.future.infer_string = True
    
    gc.collect()
    time.sleep(0.5)
    
    start_mem = get_process_memory()
    start_time = time.perf_counter()
    
    # Load DataFrame (will use PyArrow-backed strings globally under the hood!)
    df = read_dataset(PARQUET_DATASET_PATH)
    
    load_time = time.perf_counter() - start_time
    deep_mem = df.memory_usage(deep=True).sum() / (1024**2)
    process_mem_delta = get_process_memory() - start_mem
    
    print(f"  Load time: {load_time:.3f}s")
    print(f"  DataFrame Deep Memory Size: {deep_mem:.2f} MB")
    print(f"  Process Memory RSS delta: {process_mem_delta:.2f} MB")
    
    # Run operations
    timings = run_benchmark_ops(df)
    
    del df
    gc.collect()
    time.sleep(0.5)
    
    # Turn off options to restore defaults
    pd.options.mode.copy_on_write = False
    pd.options.future.infer_string = False
    
    return {
        "load_time": load_time,
        "deep_mem": deep_mem,
        "process_mem_delta": process_mem_delta,
        **timings
    }


def main():
    if not DATASET_PATH.exists():
        print(f"Error: Dataset {DATASET_PATH} not found. Please run generator or update path.")
        sys.exit(1)
        
    print(f"Benchmarking dataset: {DATASET_PATH}")
    print(f"Dataset File Size: {DATASET_PATH.stat().st_size / (1024**2):.2f} MB")
    
    # Run benchmarks
    vanilla_results = benchmark_scenario_a()
    opt_results = benchmark_scenario_b()
    
    # Output comparative results
    print("\n" + "="*70)
    print("COMPARATIVE RESULTS: VANILLA VS OPTIMIZED PANDAS")
    print("="*70)
    
    print(f"{'Metric':<25} | {'Vanilla Pandas':>18} | {'Optimized Pandas':>18} | {'Improvement':>12}")
    print("-" * 80)
    
    metrics = [
        ("load_time", "Load Time (s)", "lower", ".3f"),
        ("deep_mem", "DataFrame Deep Mem (MB)", "lower", ".2f"),
        ("process_mem_delta", "Process Mem Delta (MB)", "lower", ".2f"),
        ("filter_group", "Filter & Group (s)", "lower", ".4f"),
        ("statistics", "Statistics (s)", "lower", ".4f"),
        ("complex_join", "Complex Join (s)", "lower", ".4f"),
        ("timeseries", "Timeseries (s)", "lower", ".4f"),
    ]
    
    for key, label, direction, fmt in metrics:
        val_v = vanilla_results[key]
        val_o = opt_results[key]
        
        if direction == "lower":
            diff_pct = (val_v - val_o) / val_v * 100 if val_v else 0
        else:
            diff_pct = (val_o - val_v) / val_v * 100 if val_v else 0
            
        sign = "+" if diff_pct >= 0 else ""
        impr_label = f"{sign}{diff_pct:.1f}%"
        
        print(f"{label:<25} | {val_v:>{18},{fmt}} | {val_o:>{18},{fmt}} | {impr_label:>12}")


if __name__ == "__main__":
    main()
