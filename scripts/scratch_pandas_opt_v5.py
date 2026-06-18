"""
Benchmark Copy-on-Write and PyArrow Strings globally in Pandas.
Compares:
1. Scenario A: Vanilla Pandas (defaults, no global optimizations)
2. Scenario B: Optimized Pandas (CoW enabled + future PyArrow string inference enabled)
"""

from __future__ import annotations

import argparse
import csv
import gc
import statistics
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import psutil

project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

import pandas as pd
from utils.pandas_benchmark_ops import complex_join_top_ranked, timeseries_hour_counts

DEFAULT_DATASET = Path("data/raw/synthetic_logs_7M.parquet")
DEFAULT_OUTPUT_DIR = Path("data/results/scratch")


def read_dataset(ds: Path) -> pd.DataFrame:
    """Read dataset from CSV or Parquet file."""
    if ds.suffix == ".csv":
        return pd.read_csv(ds)
    elif ds.suffix == ".parquet":
        return pd.read_parquet(ds, engine="auto")
    else:
        raise ValueError(f"Unsupported file format: {ds.suffix}")


def get_process_memory() -> float:
    """Return resident memory used by current process in MB."""
    process = psutil.Process()
    return process.memory_info().rss / (1024**2)


@contextmanager
def pandas_optimized():
    """Context manager that enables CoW + PyArrow string inference, then restores defaults."""
    old_cow = pd.options.mode.copy_on_write
    old_infer = pd.options.future.infer_string
    pd.options.mode.copy_on_write = True
    pd.options.future.infer_string = True
    try:
        yield
    finally:
        pd.options.mode.copy_on_write = old_cow
        pd.options.future.infer_string = old_infer


def run_benchmark_ops(df: pd.DataFrame) -> dict[str, float]:
    """Run all benchmark operations and return durations."""
    timings: dict[str, float] = {}

    # 1. Filter and Group
    gc.collect()
    start = time.perf_counter()
    filtered = df[df["bytes"] > 1000]
    fg_result = filtered.groupby("event_type").size().reset_index(name="count")
    timings["filter_group"] = time.perf_counter() - start
    del filtered, fg_result

    # 2. Statistical Analysis
    gc.collect()
    start = time.perf_counter()
    stat_cols = ["bytes", "response_time_ms", "risk_score"]
    stats_result = df.groupby("event_type")[stat_cols].agg(["mean", "min", "max"]).reset_index()
    timings["statistics"] = time.perf_counter() - start
    del stats_result

    # 3. Complex Join & Windowing
    gc.collect()
    start = time.perf_counter()
    cj_result = complex_join_top_ranked(df, rank_col="bytes_rank", observed=False, sort_by_rank=True)
    timings["complex_join"] = time.perf_counter() - start
    del cj_result

    # 4. Time Series
    gc.collect()
    start = time.perf_counter()
    ts_result = timeseries_hour_counts(df, observed=False, reset_index=True, hour_name="hour")
    timings["timeseries"] = time.perf_counter() - start
    del ts_result

    gc.collect()
    return timings


def run_single_benchmark(dataset_path: Path, use_optimized: bool) -> dict[str, float]:
    """Run a single benchmark pass: load data, measure, run ops, return results."""
    gc.collect()
    time.sleep(0.5)

    start_mem = get_process_memory()
    start_time = time.perf_counter()

    if use_optimized:
        with pandas_optimized():
            df = read_dataset(dataset_path)
    else:
        df = read_dataset(dataset_path)

    load_time = time.perf_counter() - start_time
    deep_mem = df.memory_usage(deep=True).sum() / (1024**2)
    process_mem_delta = get_process_memory() - start_mem

    timings = run_benchmark_ops(df)

    del df
    gc.collect()

    return {
        "load_time": load_time,
        "deep_mem": deep_mem,
        "process_mem_delta": process_mem_delta,
        **timings,
    }


def aggregate_runs(all_runs: list[dict[str, float]]) -> dict[str, dict[str, float]]:
    """Compute mean, median, stdev across multiple runs for each metric."""
    keys = all_runs[0].keys()
    result: dict[str, dict[str, float]] = {}
    for key in keys:
        values = [r[key] for r in all_runs]
        result[key] = {
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        }
    return result


def print_comparison(vanilla: dict[str, dict[str, float]], optimized: dict[str, dict[str, float]]) -> None:
    """Print a formatted comparison table."""
    print("\n" + "=" * 80)
    print("COMPARATIVE RESULTS: VANILLA VS OPTIMIZED PANDAS (mean values)")
    print("=" * 80)

    header = f"{'Metric':<25} | {'Vanilla':>14} | {'Optimized':>14} | {'Improvement':>12}"
    print(header)
    print("-" * len(header))

    metrics = [
        ("load_time", "Load Time (s)", ".3f"),
        ("deep_mem", "DataFrame Mem (MB)", ".2f"),
        ("process_mem_delta", "RSS Delta (MB)", ".2f"),
        ("filter_group", "Filter & Group (s)", ".4f"),
        ("statistics", "Statistics (s)", ".4f"),
        ("complex_join", "Complex Join (s)", ".4f"),
        ("timeseries", "Timeseries (s)", ".4f"),
    ]

    for key, label, fmt in metrics:
        val_v = vanilla[key]["mean"]
        val_o = optimized[key]["mean"]

        if val_v:
            diff_pct = (val_v - val_o) / val_v * 100
        else:
            diff_pct = 0.0

        sign = "+" if diff_pct >= 0 else ""
        impr = f"{sign}{diff_pct:.1f}%"

        print(f"{label:<25} | {val_v:>14{fmt}} | {val_o:>14{fmt}} | {impr:>12}")


def write_results_csv(
    vanilla: dict[str, dict[str, float]],
    optimized: dict[str, dict[str, float]],
    output_path: Path,
) -> None:
    """Write aggregated results to a CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["metric", "vanilla_mean", "vanilla_median", "vanilla_stdev",
                  "optimized_mean", "optimized_median", "optimized_stdev", "improvement_pct"]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for key in vanilla:
            v_mean = vanilla[key]["mean"]
            o_mean = optimized[key]["mean"]
            improvement = ((v_mean - o_mean) / v_mean * 100) if v_mean else 0.0

            writer.writerow({
                "metric": key,
                "vanilla_mean": f"{v_mean:.6f}",
                "vanilla_median": f"{vanilla[key]['median']:.6f}",
                "vanilla_stdev": f"{vanilla[key]['stdev']:.6f}",
                "optimized_mean": f"{o_mean:.6f}",
                "optimized_median": f"{optimized[key]['median']:.6f}",
                "optimized_stdev": f"{optimized[key]['stdev']:.6f}",
                "improvement_pct": f"{improvement:.2f}",
            })

    print(f"\nResults written to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark CoW + PyArrow Strings in Pandas."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=DEFAULT_DATASET,
        help=f"Path to CSV or Parquet dataset (default: {DEFAULT_DATASET})",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of benchmark repetitions per scenario (default: 3)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR / "pandas_cow_benchmark.csv",
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_DIR}/pandas_cow_benchmark.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.dataset.exists():
        print(f"Error: Dataset {args.dataset} not found.")
        sys.exit(1)

    print(f"Benchmarking dataset: {args.dataset}")
    print(f"Dataset File Size: {args.dataset.stat().st_size / (1024**2):.2f} MB")
    print(f"Runs per scenario: {args.runs}")

    # Collect runs for each scenario
    vanilla_runs: list[dict[str, float]] = []
    optimized_runs: list[dict[str, float]] = []

    for i in range(args.runs):
        print(f"\n--- Run {i + 1}/{args.runs} ---")
        print("  [Vanilla]")
        vanilla_runs.append(run_single_benchmark(args.dataset, use_optimized=False))
        print("  [Optimized]")
        optimized_runs.append(run_single_benchmark(args.dataset, use_optimized=True))

    vanilla_agg = aggregate_runs(vanilla_runs)
    optimized_agg = aggregate_runs(optimized_runs)

    print_comparison(vanilla_agg, optimized_agg)
    write_results_csv(vanilla_agg, optimized_agg, args.output)


if __name__ == "__main__":
    main()
