"""
benchmark_05.py - Refactored comprehensive data processing benchmark.

Design goals (the "why"):
- Eliminate global mutable state by encapsulating a benchmark run in a class.
- Replace per-library function duplication with an operation registry (strategy pattern).
- Scope pandas global options (CoW, PyArrow strings) to the pandas/fireducks
  execution window instead of mutating them at module import time.
- Produce a single, honest results schema: results[library][operation].
- Separate execution, result shaping, and CSV serialization so each piece can
  be tested and reasoned about independently.
- Use the standard ``logging`` module instead of ad-hoc ``print`` calls.

This script intentionally keeps the same CSV output schema as benchmark.py so
existing downstream analysis tools (compare_hosts.py, etc.) keep working.
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import psutil

# Project root is three levels up from scripts/benchmark/benchmark_05.py.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# Reusable project/util modules. The noqa comments acknowledge the intentional
# sys.path mutation above, which is required because these scripts are executed
# directly and not installed as a package.
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
from utils.benchmark_schema import (  # noqa: E402
    BENCHMARK_OPTIMIZATION_TYPES,
    LIBRARY_ORDER,
    OPERATION_ORDER,
)
from utils.benchmark_operations import (  # noqa: E402
    BenchmarkOperation,
    ComplexJoinOperation,
    FilterGroupOperation,
    StatisticsOperation,
    TimeseriesOperation,
)
from utils.config import setup_project  # noqa: E402
from utils.data_io import DatasetFinder, UniversalDataReader, get_dataset_size  # noqa: E402
from utils.duckdb_utils import DuckDBBenchmarkSource  # noqa: E402
from utils.host_info import get_host_info  # noqa: E402
from utils.platform_utils import (  # noqa: E402
    FIREDUCKS_AVAILABLE,
    PlatformDetector,
)

# Optional libraries are imported defensively. We still need their names for
# type annotations, but ``from __future__ import annotations`` keeps those
# annotations from being evaluated at runtime.
try:
    import polars as pl
except ImportError:
    pl = None  # type: ignore[assignment]

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore[assignment]

if FIREDUCKS_AVAILABLE:
    import fireducks.pandas as fpd  # type: ignore[import]
else:
    fpd = None

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logger = logging.getLogger("benchmark_05")


def _setup_logging(level: int = logging.INFO) -> None:
    """Configure a plain-text logger suitable for benchmark progress output."""
    # We attach the handler to the module-level logger directly instead of
    # reconfiguring the root logger. Other libraries (and some execution
    # environments) may have already attached handlers to the root logger that
    # interfere with propagation; configuring our own logger keeps benchmark
    # output deterministic and easy to control.
    logger.setLevel(level)
    logger.propagate = False
    # Avoid duplicate handlers if this function is called more than once.
    logger.handlers = []
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    # Imported libraries (notably some data-processing backends) may call
    # logging.disable(...) to suppress noise. That global setting overrides
    # per-logger levels, so we reset it to ensure our progress messages emit.
    logging.disable(logging.NOTSET)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class BenchmarkRunConfig:
    """Immutable configuration for a single benchmark run."""

    dataset_path: Path | None
    output_path: Path | None
    optimize_mode: str = "auto"
    memory_threshold_gb: float = 16.0
    prep_memory_report: str = "off"
    optimized_cache_mode: str = "off"
    optimized_cache_dir: Path | None = None
    use_csv_dtype_hints: bool = True
    duckdb_mode: str = "file"
    repeat: int = 1
    log_level: str = "INFO"


# -----------------------------------------------------------------------------
# Scoped pandas options
# -----------------------------------------------------------------------------


@contextmanager
def pandas_optimized_options():
    """
    Temporarily enable pandas Copy-on-Write and PyArrow string inference.

    Why a context manager? Previous scripts mutated these globals at import
    time, which leaks side effects into any downstream code that imports the
    module. Scoping the options to the pandas/fireducks benchmark window keeps
    the rest of the process (and other libraries) unaffected.
    """
    old_cow = pd.options.mode.copy_on_write
    old_infer = pd.options.future.infer_string
    pd.options.mode.copy_on_write = True
    pd.options.future.infer_string = True
    try:
        yield
    finally:
        pd.options.mode.copy_on_write = old_cow
        pd.options.future.infer_string = old_infer


# -----------------------------------------------------------------------------
# Result types
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class OperationResult:
    """Outcome of benchmarking one (library, operation) pair."""

    status: str  # "success", "skipped", or "error"
    duration: float | None = None
    memory_delta_mb: float | None = None
    result_shape: str | None = None
    reason: str | None = None


@dataclass
class BenchmarkRunResults:
    """Container for all results produced by a benchmark run."""

    # results[library][operation_name] = OperationResult
    results: dict[str, dict[str, OperationResult]] = field(default_factory=dict)

    def get(self, library: str, operation: str) -> OperationResult | None:
        return self.results.get(library, {}).get(operation)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _shape_of(result: Any) -> str:
    """Return a human-readable shape string for any result object."""
    if result is None:
        return "None"
    if hasattr(result, "shape"):
        return str(result.shape)
    if hasattr(result, "__len__"):
        try:
            return f"({len(result)},)"
        except Exception:
            pass
    return "N/A"


def _dataset_format(dataset_path: Path) -> str:
    """
    Normalize a dataset filename to a logical format name for the results CSV.

    Compression suffixes (.gz, .zip, .zst, .bz2) are stripped, and jsonl/ndjson
    are mapped to ``ndjson`` so downstream comparisons are format-consistent.
    """
    suffixes = [s.lower() for s in dataset_path.suffixes]
    compression = {".gz", ".zip", ".zst", ".bz2"}
    base = [s for s in suffixes if s not in compression]
    ext = (base[-1] if base else dataset_path.suffix).lower().lstrip(".")
    if ext in ("jsonl", "ndjson"):
        return "ndjson"
    return ext or "unknown"


# -----------------------------------------------------------------------------
# Operation registry
# -----------------------------------------------------------------------------


# Registry of all benchmark operations. Adding a new operation is now a matter
# of implementing one class and appending it to this list. The concrete
# operation logic lives in ``utils.benchmark_operations`` so it can be reused
# by benchmark.py, benchmark_01.py, benchmark_02.py, and benchmark_modular.py.
OPERATIONS: list[BenchmarkOperation] = [
    FilterGroupOperation(),
    StatisticsOperation(),
    ComplexJoinOperation(rank_col="total_rank", sort_by_rank=False),
    TimeseriesOperation(reset_index=False, hour_name="_hour"),
]


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------


class BenchmarkRunner:
    """Orchestrates loading data once per library and timing every operation."""

    def __init__(self, config: BenchmarkRunConfig):
        self.config = config
        self.platform = PlatformDetector()
        self.data_reader = UniversalDataReader(default_library="pandas")
        self.duckdb_source = DuckDBBenchmarkSource(config.duckdb_mode)
        self.optimization_decision = decide_memory_optimization(
            config.optimize_mode, config.memory_threshold_gb
        )
        self._optimization_message_printed: set[str] = set()

        # Resolve the dataset path and confirm it is readable.
        self.dataset_path = self._resolve_dataset()
        self.dataset_size = get_dataset_size(self.dataset_path)
        if self.dataset_size == 0:
            logger.warning("Dataset appears empty or unreadable: %s", self.dataset_path)

    # -------------------------------------------------------------------------
    # Dataset resolution
    # -------------------------------------------------------------------------

    def _resolve_dataset(self) -> Path:
        """Return the dataset path from CLI, environment, or auto-detection."""
        if self.config.dataset_path is not None:
            path = self.config.dataset_path
            if not path.exists():
                raise FileNotFoundError(f"Specified dataset not found: {path}")
            if not path.is_file():
                raise ValueError(f"Specified path is not a file: {path}")
            return path

        env_path = os.environ.get("BENCHMARK_DATASET")
        if env_path:
            path = Path(env_path)
            if path.exists() and path.is_file():
                return path

        project_config = setup_project(PROJECT_ROOT)
        finder = DatasetFinder(
            search_dirs=project_config.get_dataset_search_dirs(),
            file_patterns=project_config.dataset_patterns,
        )
        discovered = finder.find_dataset(PROJECT_ROOT)
        if discovered is None or not discovered.exists():
            raise FileNotFoundError(
                "No suitable dataset found. Use -d/--dataset or set BENCHMARK_DATASET."
            )
        return discovered

    # -------------------------------------------------------------------------
    # Loading
    # -------------------------------------------------------------------------

    def _print_optimization_decision_once(self, library: str) -> None:
        """Print the memory-optimization decision once per pandas-like library."""
        if library not in self._optimization_message_printed:
            logger.info("  %s", self.optimization_decision.message)
            self._optimization_message_printed.add(library)

    def _load_pandas_like(self, library: str) -> pd.DataFrame:
        """Load and optionally optimize a pandas-compatible DataFrame."""
        cache_dir = self.config.optimized_cache_dir
        if cache_dir is None:
            cache_dir = PROJECT_ROOT / "data" / "cache" / "optimized"

        self._print_optimization_decision_once(library)
        return load_pandas_like_for_benchmark(
            self.dataset_path,
            library=library,
            type_map=BENCHMARK_OPTIMIZATION_TYPES,
            should_optimize=self.optimization_decision.should_optimize,
            prep_memory_report=self.config.prep_memory_report,
            optimized_cache_mode=self.config.optimized_cache_mode,
            optimized_cache_dir=cache_dir,
            use_dtype_hints=self.config.use_csv_dtype_hints,
        )

    def _load_polars(self) -> pl.DataFrame:
        """Read the dataset into a Polars DataFrame, recording prep time."""
        fmt = self.data_reader.detect_file_format(self.dataset_path)
        prep_start = time.perf_counter()
        step_start = prep_start

        if fmt == "csv":
            df = pl.read_csv(self.dataset_path)
        elif fmt == "parquet":
            df = pl.read_parquet(self.dataset_path)
        elif fmt == "json":
            df = pl.read_json(self.dataset_path)
        elif fmt == "ndjson":
            try:
                df = pl.read_ndjson(self.dataset_path)
            except Exception:
                df = pl.read_json(self.dataset_path, lines=True)
        else:
            df = pl.read_csv(self.dataset_path)

        print_prep_timing("polars", "read/load", step_start)
        print_prep_timing("polars", "total load/optimization", prep_start)
        return df

    def _prepare_duckdb(self) -> None:
        """Prepare DuckDB in either file-scan or cached-table mode."""
        prep_start = time.perf_counter()
        elapsed = self.duckdb_source.prepare(self.dataset_path)
        if elapsed is None:
            record_prep_timing("duckdb", "total load/optimization", 0.0)
            logger.info("  [duckdb prep] total load/optimization: 0.000s (file scan mode)")
        else:
            record_prep_timing("duckdb", "read/load cached table", elapsed)
            logger.info("  [duckdb prep] read/load cached table: %.3fs", elapsed)
            print_prep_timing("duckdb", "total load/optimization", prep_start)

    # -------------------------------------------------------------------------
    # Execution
    # -------------------------------------------------------------------------

    def _dispatch_operation(self, operation: BenchmarkOperation, library: str, data: Any) -> Any:
        """Call the operation method that corresponds to the requested library."""
        if library == "pandas":
            return operation.run_pandas(data)
        if library == "fireducks":
            return operation.run_fireducks(data)
        if library == "polars":
            return operation.run_polars(data)
        raise ValueError(f"Unknown in-memory library: {library}")

    def _run_operation(
        self,
        operation: BenchmarkOperation,
        library: str,
        data: Any,
    ) -> OperationResult:
        """Time and memory-profile a single operation, with ``repeat`` averaging."""
        process = psutil.Process()
        rss_before = process.memory_info().rss
        start_all = time.perf_counter()

        try:
            result = None
            for _ in range(self.config.repeat):
                if library == "duckdb":
                    with self.duckdb_source.query(self.dataset_path) as (con, expr):
                        result = operation.run_duckdb(expr, con)
                else:
                    result = self._dispatch_operation(operation, library, data)

            duration = (time.perf_counter() - start_all) / max(self.config.repeat, 1)
            rss_after = process.memory_info().rss

            if result is None:
                # A None result from the operation method means required columns
                # were missing; treat this as a skip rather than a failure.
                return OperationResult(
                    status="skipped",
                    reason="required columns missing",
                )

            return OperationResult(
                status="success",
                duration=duration,
                memory_delta_mb=(rss_after - rss_before) / (1024**2),
                result_shape=_shape_of(result),
            )
        except Exception as exc:  # noqa: BLE001 - benchmark harness must be resilient
            return OperationResult(
                status="error",
                reason=f"{type(exc).__name__}: {exc}",
            )

    def _release_data(self, data: Any, library: str) -> None:
        """Explicitly drop a cached DataFrame and hint the garbage collector."""
        logger.info("  Releasing cached %s dataset...", library)
        del data
        step_start = time.perf_counter()
        gc.collect()
        print_prep_timing(library, "gc after release", step_start)

    def _run_library(self, library: str) -> dict[str, OperationResult]:
        """Load data once for a library, run all operations, then clean up."""
        logger.info("\n%s", "=" * 60)
        logger.info("BENCHMARKING %s", library.upper())
        logger.info("%s", "=" * 60)

        if library in ("pandas", "fireducks"):
            with pandas_optimized_options():
                data = self._load_pandas_like(library)
                results = {
                    operation.name: self._run_operation(operation, library, data)
                    for operation in OPERATIONS
                }
                self._release_data(data, library)
        elif library == "polars":
            data = self._load_polars()
            results = {
                operation.name: self._run_operation(operation, library, data)
                for operation in OPERATIONS
            }
            self._release_data(data, library)
        elif library == "duckdb":
            self._prepare_duckdb()
            results = {
                operation.name: self._run_operation(operation, library, None)
                for operation in OPERATIONS
            }
            self.duckdb_source.close()
        else:
            raise ValueError(f"Unsupported library: {library}")

        return results

    def run(self) -> BenchmarkRunResults:
        """Run the benchmark for every available library and return results."""
        reset_prep_timings()
        available = self.platform.get_available_benchmark_libraries()
        logger.info("Available libraries: %s", ", ".join(available))

        all_results = BenchmarkRunResults()
        for library in LIBRARY_ORDER:
            if library not in available:
                logger.info("\n%s not available; skipping.", library.title())
                continue
            all_results.results[library] = self._run_library(library)

        return all_results


# -----------------------------------------------------------------------------
# CSV writer
# -----------------------------------------------------------------------------


class BenchmarkResultsWriter:
    """Write benchmark results to the project-wide CSV schema."""

    # CSV column order shared with benchmark.py / benchmark_01.py so that
    # compare_hosts.py and other analysis tools work without modification.
    HOST_COLUMNS = [
        "timestamp",
        "hostname",
        "platform",
        "system",
        "release",
        "version",
        "machine",
        "processor",
        "cpu_count_logical",
        "cpu_count_physical",
        "cpu_freq_max",
        "cpu_freq_current",
        "memory_total_gb",
        "memory_available_gb",
        "python_version",
        "python_implementation",
        "cpu_brand",
        "cpu_arch",
    ]

    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def _build_header(self) -> list[str]:
        """Construct the wide-format CSV header."""
        timing_keys = [
            f"{op}_{lib}_seconds"
            for op in OPERATION_ORDER
            for lib in LIBRARY_ORDER
        ]
        return (
            self.HOST_COLUMNS
            + ["dataset_size", "dataset_name", "dataset_format"]
            + timing_keys
            + PREP_COLUMNS
            + ["script_name"]
        )

    def _timing_value(self, result: OperationResult | None) -> str:
        """Return a CSV cell value for a single operation result."""
        if result is None or result.status != "success" or result.duration is None:
            return ""
        return f"{result.duration:.6f}"

    def write(
        self,
        results: BenchmarkRunResults,
        host_info: dict[str, Any],
        script_name: str,
        dataset_path: Path,
        dataset_size: int,
    ) -> None:
        """Serialize results to CSV, appending to existing history."""
        header = self._build_header()

        row = [host_info.get(col, "") for col in self.HOST_COLUMNS]
        row.extend(
            [
                dataset_size,
                dataset_path.name,
                _dataset_format(dataset_path),
            ]
        )

        for op in OPERATION_ORDER:
            for lib in LIBRARY_ORDER:
                row.append(self._timing_value(results.get(lib, op)))

        row.extend(get_prep_csv_values())
        row.append(script_name)

        append_csv_row_with_schema(self.output_path, header, row)
        logger.info("Results saved to: %s", self.output_path)


# -----------------------------------------------------------------------------
# Summary printer
# -----------------------------------------------------------------------------


def print_summary(results: BenchmarkRunResults) -> None:
    """Print a concise, sorted summary of successful timings."""
    logger.info("\n%s", "=" * 60)
    logger.info("BENCHMARK SUMMARY")
    logger.info("%s", "=" * 60)

    for op in OPERATIONS:
        logger.info("\n%s Operation:", op.name.upper().replace("_", " "))
        timings: dict[str, float] = {}
        for lib in LIBRARY_ORDER:
            res = results.get(lib, op.name)
            if res and res.status == "success" and res.duration is not None and res.duration > 0:
                timings[lib] = res.duration

        if not timings:
            logger.info("  No valid timings.")
            continue

        fastest_lib, fastest_time = min(timings.items(), key=lambda x: x[1])
        for lib, duration in sorted(timings.items(), key=lambda x: x[1]):
            if lib == fastest_lib:
                logger.info("  %-10s: %.4fs (fastest)", lib, duration)
            else:
                logger.info(
                    "  %-10s: %.4fs (%.4fs slower than %s)",
                    lib,
                    duration,
                    duration - fastest_time,
                    fastest_lib,
                )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def _parse_args() -> BenchmarkRunConfig:
    """Parse command-line arguments into an immutable configuration object."""
    parser = argparse.ArgumentParser(
        description="Refactored comprehensive data processing benchmark."
    )
    parser.add_argument(
        "-d",
        "--dataset",
        type=Path,
        default=None,
        help="Path to dataset file (overrides auto-detection)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Results CSV output path (default: data/benchmark_results.csv)",
    )
    parser.add_argument(
        "-r",
        "--repeat",
        type=int,
        default=1,
        help="Repeat each operation N times and report the mean (default: 1)",
    )
    parser.add_argument(
        "--optimize",
        "-opt",
        choices=["auto", "always", "never"],
        default="auto",
        help="Memory optimization mode for pandas/fireducks (default: auto)",
    )
    parser.add_argument(
        "--mem-threshold",
        "-m",
        type=float,
        default=16.0,
        help="Memory threshold in GB for --optimize auto (default: 16)",
    )
    parser.add_argument(
        "--prep-memory-report",
        choices=["off", "shallow", "deep"],
        default="off",
        help="Measure pandas/fireducks memory before/after optimization (default: off)",
    )
    parser.add_argument(
        "--optimized-cache",
        choices=["off", "read", "write", "readwrite", "refresh"],
        default="off",
        help="Use optimized pandas/fireducks parquet cache (default: off)",
    )
    parser.add_argument(
        "--optimized-cache-dir",
        type=Path,
        default=None,
        help="Directory for optimized cache files",
    )
    parser.add_argument(
        "--no-csv-dtype-hints",
        action="store_true",
        help="Disable dtype hints while loading CSV with pandas/fireducks",
    )
    parser.add_argument(
        "--duckdb-mode",
        choices=["file", "cached"],
        default="file",
        help="DuckDB source mode: file scans per query, cached loads once (default: file)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity (default: INFO)",
    )

    args = parser.parse_args()

    if args.repeat < 1:
        parser.error("--repeat must be >= 1")

    output_path = args.output
    if output_path is None:
        output_path = PROJECT_ROOT / "data" / "benchmark_results.csv"

    return BenchmarkRunConfig(
        dataset_path=args.dataset,
        output_path=output_path,
        optimize_mode=args.optimize,
        memory_threshold_gb=args.mem_threshold,
        prep_memory_report=args.prep_memory_report,
        optimized_cache_mode=args.optimized_cache,
        optimized_cache_dir=args.optimized_cache_dir,
        use_csv_dtype_hints=not args.no_csv_dtype_hints,
        duckdb_mode=args.duckdb_mode,
        repeat=args.repeat,
        log_level=args.log_level,
    )


def _display_system_info(host_info: dict[str, Any]) -> None:
    """Log the host/system preamble."""
    logger.info("=" * 60)
    logger.info("COMPREHENSIVE DATA PROCESSING BENCHMARK")
    logger.info("=" * 60)
    logger.info(
        "Running on: %s (%s)",
        host_info.get("hostname", "Unknown"),
        host_info.get("system", "Unknown"),
    )
    logger.info(
        "CPU: %s (%s logical cores)",
        host_info.get("cpu_brand", "Unknown"),
        host_info.get("cpu_count_logical", "N/A"),
    )
    logger.info(
        "Memory: %.2f GB total",
        float(host_info.get("memory_total_gb", 0.0) or 0.0),
    )


def _display_settings(config: BenchmarkRunConfig, decision) -> None:
    """Log the active benchmark settings."""
    logger.info("\nPreparation settings:")
    logger.info("  - Optimization mode: %s", config.optimize_mode)
    if config.optimize_mode == "auto":
        logger.info("  - Auto threshold: %.1f GB", config.memory_threshold_gb)
    logger.info("  - Prep memory report: %s", config.prep_memory_report)
    logger.info("  - Optimized cache: %s", config.optimized_cache_mode)
    logger.info("  - CSV dtype hints: %s", "enabled" if config.use_csv_dtype_hints else "disabled")
    logger.info("  - DuckDB mode: %s", config.duckdb_mode)
    logger.info("  - Repeat per operation: %d", config.repeat)
    logger.info("  - %s", decision.message)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------


def main() -> int:
    """CLI entry point. Returns an exit code for the shell."""
    config = _parse_args()
    _setup_logging(getattr(logging, config.log_level))

    host_info = get_host_info()
    _display_system_info(host_info)

    decision = decide_memory_optimization(
        config.optimize_mode, config.memory_threshold_gb
    )
    _display_settings(config, decision)

    try:
        runner = BenchmarkRunner(config)
    except FileNotFoundError as exc:
        logger.error("Dataset error: %s", exc)
        return 1

    logger.info("\nUsing dataset: %s", runner.dataset_path)
    logger.info("Dataset size: %s rows", f"{runner.dataset_size:,}")

    # Memory sanity check before starting heavy work.
    available_gb = psutil.virtual_memory().available / (1024**3)
    if available_gb < 4:
        logger.warning("Only %.1f GB available; operations may fail.", available_gb)

    results = runner.run()

    script_name = build_script_name(
        Path(__file__).name,
        config.optimize_mode,
        decision.should_optimize,
        decision.total_memory_gb,
    )

    writer = BenchmarkResultsWriter(config.output_path)
    writer.write(
        results,
        host_info,
        script_name,
        runner.dataset_path,
        runner.dataset_size,
    )

    print_summary(results)
    logger.info("\nBenchmark completed successfully.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.info("\nBenchmark interrupted by user.")
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001 - top-level safety net
        logger.exception("Critical error in benchmark: %s", exc)
        sys.exit(1)
