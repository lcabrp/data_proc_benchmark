# Refactored using qweb2.5:7b

import gc
import sys
import time
from pathlib import Path
import psutil

# Add project root to Python path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

import pandas as pd
from utils.pandas_benchmark_ops import complex_join_top_ranked, timeseries_hour_counts

DATASET_PATH = Path("data/raw/synthetic_logs_10M.csv")

def get_process_memory():
    """Return resident memory used by current process in MB."""
    process = psutil.Process()
    return process.memory_info().rss / (1024**2)

def run_benchmark_ops(df, operation_name):
    """Run a specific benchmark operation and return its duration + memory stats."""
    gc.collect()  # Ensure garbage collection before measurement
    start_time = time.perf_counter()
    
    if operation_name == 'filter_group':
        filtered = df[df['bytes'] > 1000]
        fg_result = filtered.groupby('event_type').size().reset_index(name='count')
        del filtered, fg_result
    elif operation_name == 'statistics':
        stat_cols = ['bytes', 'response_time_ms', 'risk_score']
        stats_result = df.groupby('event_type')[stat_cols].agg(['mean', 'min', 'max']).reset_index()
        del stats_result
    elif operation_name == 'complex_join':
        cj_result = complex_join_top_ranked(df, rank_col="bytes_rank", observed=False, sort_by_rank=True)
        del cj_result
    elif operation_name == 'timeseries':
        ts_result = timeseries_hour_counts(df, observed=False, reset_index=True, hour_name="hour")
        del ts_result
    
    end_time = time.perf_counter()
    
    # Measure memory usage once per operation
    initial_memory = get_process_memory()
    gc.collect()  # Collect garbage before final measurement
    final_memory = get_process_memory()
    
    return {
        'operation': operation_name,
        'duration': end_time - start_time,
        'initial_memory': initial_memory,
        'final_memory': final_memory
    }

def benchmark_scenario_a():
    """Benchmark Vanilla Pandas."""
    print("\n--- Running Scenario A: Vanilla Pandas ---")
    
    # Load DataFrame normally and time it
    start_load = time.perf_counter()
    df = pd.read_csv(DATASET_PATH)
    end_load = time.perf_counter()
    load_time = end_load - start_load

    operations = ['filter_group', 'statistics', 'complex_join', 'timeseries']
    
    results = {op: {} for op in operations}
    
    for operation_name in operations:
        print(f"  Running {operation_name}...")
        result = run_benchmark_ops(df, operation_name)
        
        # Store the result
        results[operation_name] = {
            'load_time': 0,
            'deep_mem': (result['final_memory'] - result['initial_memory']) / (1024**2),
            'process_mem_delta': result['final_memory'] - result['initial_memory']
        }
    
    del df
    gc.collect()
    return load_time, results

def benchmark_scenario_b():
    """Benchmark Optimized Pandas (CoW + PyArrow Strings)."""
    print("\n--- Running Scenario B: Optimized Pandas (CoW + PyArrow Strings) ---")
    
    # Use set_option instead of direct attribute access to avoid DictWrapper issues
    pd.set_option('mode.copy_on_write', True)
    pd.set_option('future.infer_string', True)
    
    gc.collect()
    
    # Load DataFrame normally and time it
    start_load = time.perf_counter()
    df = pd.read_csv(DATASET_PATH)
    end_load = time.perf_counter()
    load_time = end_load - start_load

    operations = ['filter_group', 'statistics', 'complex_join', 'timeseries']
    
    results = {op: {} for op in operations}
    
    for operation_name in operations:
        print(f"  Running {operation_name}...")
        result = run_benchmark_ops(df, operation_name)
        
        # Store the result
        results[operation_name] = {
            'load_time': 0,
            'deep_mem': (result['final_memory'] - result['initial_memory']) / (1024**2),
            'process_mem_delta': result['final_memory'] - result['initial_memory']
        }
    
    # Reset options safely
    pd.set_option('mode.copy_on_write', False)
    pd.set_option('future.infer_string', False)
    
    del df
    gc.collect()
    return load_time, results

def main():
    if not DATASET_PATH.exists():
        print(f"Error: Dataset {DATASET_PATH} not found. Please run generator or update path.")
        sys.exit(1)
    
    print(f"Benchmarking dataset: {DATASET_PATH}")
    print(f"Dataset File Size: {DATASET_PATH.stat().st_size / (1024**2):.2f} MB")
    
    # Run benchmarks
    vanilla_load_time, vanilla_results = benchmark_scenario_a()
    opt_load_time, opt_results = benchmark_scenario_b()
    
    # Calculate average operational metrics for comparison summary
    OP_METRICS = ['deep_mem', 'process_mem_delta']

    def calculate_average(results_dict):
        if not results_dict:
            return {'deep_mem': 0.0, 'process_mem_delta': 0.0}
        
        # Deep Mem and Process Mem Delta are calculated for all 4 operations
        avg_deep = sum(op['deep_mem'] for op in results_dict.values()) / len(results_dict)
        avg_proc = sum(op['process_mem_delta'] for op in results_dict.values()) / len(results_dict)
        return {'deep_mem': avg_deep, 'process_mem_delta': avg_proc}

    vanilla_summary = calculate_average(vanilla_results)
    opt_summary = calculate_average(opt_results)


    # Output comparative results
    print("\n" + "="*70)
    print("COMPARATIVE RESULTS: VANILLA VS OPTIMIZED PANDAS")
    print("="*70)
    
    print(f"{'Metric':<25} | {'Vanilla Pandas':>18} | {'Optimized Pandas':>18} | {'Improvement':>12}")
    print("-" * 80)
    
    metrics = [
        ("load_time", "Load Time (s)", "lower", ".3f"),
        ("deep_mem", "Avg Deep Mem Delta (MB)", "lower", ".2f"),
        ("process_mem_delta", "Avg Process Mem Delta (MB)", "lower", ".2f")
    ]
    
    for metric_key, metric_label, _, fmt in metrics:
        if metric_key == 'load_time':
            vanilla_val = vanilla_load_time
            opt_val = opt_load_time
        elif metric_key == 'deep_mem':
            vanilla_val = vanilla_summary['deep_mem']
            opt_val = opt_summary['deep_mem']
        elif metric_key == 'process_mem_delta':
            vanilla_val = vanilla_summary['process_mem_delta']
            opt_val = opt_summary['process_mem_delta']
        else: # Should not happen
            continue

        if metric_key == 'load_time':
            # Improvement calculation is different for load time vs operations
            diff_pct = (vanilla_val - opt_val) / vanilla_val * 100 if vanilla_val else 0
        else:
            # Improvement calculation based on magnitude difference for memory metrics
            diff_pct = (opt_val - vanilla_val) / vanilla_val * 100 if vanilla_val and opt_val != 0 else 0


        sign = "+" if diff_pct >= 0 else ""
        impr_label = f"{sign}{diff_pct:.1f}%"
        
        print(f"{metric_label:<25} | {vanilla_val:>{18},{fmt}} | {opt_val:>{18},{fmt}} | {impr_label:>12}")

if __name__ == "__main__":
    main()