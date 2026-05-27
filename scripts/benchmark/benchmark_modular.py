# Replace imports section (lines 1-57) with:

"""
Modern benchmark script using reusable modular components.

This demonstrates how to build maintainable, reusable code that follows
the DRY principle and can be easily adapted for other projects.
"""

import sys
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
import time
import gc
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, cast
import argparse

import warnings
import psutil
warnings.filterwarnings("ignore", category=SyntaxWarning, message=r"invalid escape sequence \\_")

# Add project root to Python path so we can import utils modules
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Setup environment and import reusable modules
from utils.config import setup_project  # noqa: E402
from utils.data_io import UniversalDataReader, DatasetFinder  # noqa: E402
from utils.benchmark_prep import (  # noqa: E402
    PREP_COLUMNS,
    append_csv_row_with_schema,
    decide_memory_optimization,
    get_prep_csv_values,
    load_pandas_like_for_benchmark,
    print_prep_timing,
    record_prep_timing,
    reset_prep_timings,
)
from utils.duckdb_utils import DuckDBBenchmarkSource, duckdb_table_expr  # noqa: E402
from utils.host_info import get_host_info  # noqa: E402
from utils.pandas_benchmark_ops import complex_join_top_ranked, timeseries_hour_counts  # noqa: E402
from utils.useful_functions import optimize_df_types  # noqa: E402

# Import platform detection with all libraries and flags
from utils.platform_utils import (  # noqa: E402
    PlatformDetector,
    FIREDUCKS_AVAILABLE,
    POLARS_AVAILABLE, 
    DUCKDB_AVAILABLE
)

import pandas as pd  # noqa: E402

# Import optional libraries based on platform detection

if POLARS_AVAILABLE:
    import polars as pl
else:
    pl = None

BENCHMARK_OPTIMIZATION_TYPES = {
    'datetime64[ns]': ['timestamp'],
    'category': ['source_ip', 'destination_ip', 'protocol', 'event_type',
                 'severity', 'user', 'status_code', 'country', 'device_type'],
    'uint32': ['bytes', 'session_id'],
    'uint16': ['response_time_ms', 'port'],
    'float32': ['risk_score']
}


def _print_prep_timing(library: str, step: str, start_time: float) -> None:
    """Print elapsed time for a dataset preparation step."""
    print_prep_timing(library, step, start_time)


def get_result_shape(result):
    """Return shape tuple or descriptive string for any result object."""
    try:
        if result is None:
            return 'None'
        elif hasattr(result, 'shape'):
            return result.shape
        elif hasattr(result, '__len__'):
            try:
                length = len(result)
                # For pandas Series, we can get more info
                if hasattr(result, 'dtype'):
                    return f'({length},)'
                else:
                    return f'({length},)'
            except Exception:
                return 'N/A'
        else:
            return 'N/A'
    except Exception:
        return 'N/A'
        
class ModularBenchmark:
    """
    Modular benchmark class that demonstrates reusable design patterns.
    
    This class can be easily adapted for different projects by:
    - Changing the operations being benchmarked
    - Adding new data processing libraries
    - Modifying the data sources
    - Customizing the output format
    """
    
    def __init__(
        self,
        config=None,
        dataset_path: Optional[Union[str, Path]] = None,
        prep_memory_report: str = "off",
        optimized_cache_mode: str = "off",
        optimized_cache_dir: Optional[Union[str, Path]] = None,
        use_csv_dtype_hints: bool = True,
        optimize_mode: str = "auto",
        memory_threshold_gb: float = 16.0,
        duckdb_mode: str = "file",
    ):
        """Initialize the benchmark with configuration.

        Args:
            config: Optional pre-built project config.
            dataset_path: Optional explicit dataset path override (CLI -d/--dataset).
        """
        self.config = config or setup_project()
        self.prep_memory_report = prep_memory_report
        self.optimized_cache_mode = optimized_cache_mode
        self.optimized_cache_dir = (
            Path(optimized_cache_dir)
            if optimized_cache_dir is not None
            else self.config.project_root / "data" / "cache" / "optimized"
        )
        self.use_csv_dtype_hints = use_csv_dtype_hints
        self.optimize_mode = optimize_mode
        self.memory_threshold_gb = memory_threshold_gb
        self.optimization_decision = decide_memory_optimization(
            optimize_mode,
            memory_threshold_gb,
        )
        self._optimization_message_printed: set[str] = set()
        self.duckdb_source = DuckDBBenchmarkSource(duckdb_mode)
        self.data_reader = UniversalDataReader(default_library='pandas')
        self.dataset_finder = DatasetFinder(
            search_dirs=self.config.get_dataset_search_dirs(),
            file_patterns=self.config.dataset_patterns
        )

        # Resolve dataset path: explicit override > auto-detect
        resolved: Optional[Path]
        if dataset_path:
            resolved = Path(dataset_path)
            if not resolved.exists():
                raise FileNotFoundError(f"Specified dataset {resolved} does not exist.")
            if not resolved.is_file():
                raise ValueError(f"Specified path {resolved} is not a file.")
        else:
            resolved = None
        if resolved is None:
            resolved = self.dataset_finder.find_dataset(self.config.project_root)
        if not resolved or not resolved.exists():
            raise FileNotFoundError("No suitable dataset found!")
        
        # Validate dataset is readable
        try:
            with open(resolved, 'r', encoding='utf-8', errors='ignore') as f:
                f.read(1)  # Just check if readable
        except Exception as e:
            raise ValueError(f"Dataset {resolved} is not readable: {e}")
        
        self.dataset_path = resolved
        print(f"Using dataset: {self.dataset_path}")
        
        # Get dataset size after confirming path is valid
        self.dataset_size = self._get_dataset_size()
        if self.dataset_size == 0:
            print("Warning: Dataset appears empty or unreadable.")
        
        # Setup available libraries
        self.available_libraries = self._detect_available_libraries()
        
    def _get_dataset_size(self) -> int:
        """Get dataset size using the universal reader utilities."""
        try:
            # Use the get_dataset_size utility function which handles different formats properly
            from utils.data_io import get_dataset_size
            return get_dataset_size(self.dataset_path)
        except Exception as e:
            print(f"Warning: Could not determine dataset size: {e}")
            return 0
    
    def _detect_available_libraries(self) -> Dict[str, bool]:
        """Detect which libraries are available."""
        return {
            'pandas': True,
            'polars': POLARS_AVAILABLE,
            'duckdb': DUCKDB_AVAILABLE,
            'fireducks': FIREDUCKS_AVAILABLE
        }

    def _print_optimization_decision_once(self, library: str) -> None:
        """Print pandas-like optimization decision once per library."""
        if library not in self._optimization_message_printed:
            print(f"  {self.optimization_decision.message}")
            self._optimization_message_printed.add(library)
    
    # Optimization for pandas/fireducks
    def optimize_benchmark_df(self, bdf: pd.DataFrame) -> pd.DataFrame:
        """
        Apply custom dtype optimization rules for memory efficiency.
        Matches benchmark.py optimizations.
        """
        return optimize_df_types(bdf, BENCHMARK_OPTIMIZATION_TYPES)

    def load_and_optimize_pandas(self, csv_path: str) -> pd.DataFrame:
        """
        Load and optimize pandas DataFrame once.
        """
        self._print_optimization_decision_once("pandas")
        df = load_pandas_like_for_benchmark(
            Path(csv_path),
            library="pandas",
            type_map=BENCHMARK_OPTIMIZATION_TYPES,
            should_optimize=self.optimization_decision.should_optimize,
            prep_memory_report=self.prep_memory_report,
            optimized_cache_mode=self.optimized_cache_mode,
            optimized_cache_dir=self.optimized_cache_dir,
            use_dtype_hints=self.use_csv_dtype_hints,
        )
        step_start = time.perf_counter()
        gc.collect()
        _print_prep_timing("pandas", "gc after optimization", step_start)
        return df

    def _read_polars(self, path: str) -> pl.DataFrame:
        """
        Read dataset into Polars DataFrame.
        """
        reader = UniversalDataReader()
        fmt = reader.detect_file_format(Path(path))
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

    def load_and_optimize_fireducks(self, csv_path: str) -> pd.DataFrame:
        """
        Load and optimize FireDucks DataFrame once.
        """
        if not FIREDUCKS_AVAILABLE:
            raise RuntimeError("FireDucks not available")
        self._print_optimization_decision_once("fireducks")
        df = load_pandas_like_for_benchmark(
            Path(csv_path),
            library="fireducks",
            type_map=BENCHMARK_OPTIMIZATION_TYPES,
            should_optimize=self.optimization_decision.should_optimize,
            prep_memory_report=self.prep_memory_report,
            optimized_cache_mode=self.optimized_cache_mode,
            optimized_cache_dir=self.optimized_cache_dir,
            use_dtype_hints=self.use_csv_dtype_hints,
        )
        step_start = time.perf_counter()
        gc.collect()
        _print_prep_timing("fireducks", "gc after optimization", step_start)
        return df

    def _duckdb_table_expr(self):
        """Generate DuckDB table expression for the dataset."""
        return duckdb_table_expr(self.dataset_path)

    # =====================================================
    # BENCHMARK OPERATIONS - Updated to match other modules exactly
    # =====================================================
    
    def filter_group_pandas(self, df=None):
        """Filter bytes > 1000, group by event_type, count."""
        if df is None:
            df = self.load_and_optimize_pandas(str(self.dataset_path))
        if "bytes" not in df.columns or "event_type" not in df.columns:
            return None
        filtered = df[df["bytes"] > 1000]
        return filtered.groupby("event_type", observed=False).size()
    
    def filter_group_polars(self, df=None):
        """Filter bytes > 1000, group by event_type, count."""
        if df is None:
            df = self._read_polars(str(self.dataset_path))
        if not {"bytes", "event_type"}.issubset(set(df.columns)):
            return None
        return df.filter(pl.col("bytes") > 1000).group_by("event_type").agg(pl.len().alias("count"))
    
    def filter_group_duckdb(self):
        """Filter bytes > 1000, group by event_type, count."""
        if not DUCKDB_AVAILABLE:
            return None
        with self.duckdb_source.query(self.dataset_path) as (con, expr):
            # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
            # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
            return con.execute(f"""
                SELECT event_type, COUNT(*) AS count
                FROM {expr}
                WHERE bytes > 1000
                GROUP BY event_type
            """).fetch_arrow_table()
    
    def filter_group_fireducks(self, df=None):
        """Filter bytes > 1000, group by event_type, count."""
        if df is None:
            df = self.load_and_optimize_fireducks(str(self.dataset_path))
        if "bytes" not in df.columns or "event_type" not in df.columns:
            return None
        return df[df["bytes"] > 1000].groupby("event_type").size()
    
    def statistics_pandas(self, df=None):
        """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score."""
        if df is None:
            df = self.load_and_optimize_pandas(str(self.dataset_path))
        req = {"event_type", "bytes", "response_time_ms", "risk_score"}
        if not req.issubset(df.columns):
            return None
        return df.groupby("event_type", observed=False).agg({
            "bytes": ["mean", "min", "max"],
            "response_time_ms": ["mean", "min", "max"],
            "risk_score": ["mean", "min", "max"]
        })
    
    def statistics_polars(self, df=None):
        """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score."""
        if df is None:
            df = self._read_polars(str(self.dataset_path))
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
    
    def statistics_duckdb(self):
        """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score."""
        if not DUCKDB_AVAILABLE:
            return None
        with self.duckdb_source.query(self.dataset_path) as (con, expr):
            # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
            # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
            return con.execute(f"""
                SELECT event_type,
                       AVG(bytes) AS bytes_mean, MIN(bytes) AS bytes_min, MAX(bytes) AS bytes_max,
                       AVG(response_time_ms) AS response_time_ms_mean,
                       MIN(response_time_ms) AS response_time_ms_min,
                       MAX(response_time_ms) AS response_time_ms_max,
                       AVG(risk_score) AS risk_score_mean,
                       MIN(risk_score) AS risk_score_min,
                       MAX(risk_score) AS risk_score_max
                FROM {expr}
                GROUP BY event_type
            """).fetch_arrow_table()
    
    def statistics_fireducks(self, df=None):
        """Group by event_type, mean/min/max for bytes, response_time_ms, risk_score."""
        if df is None:
            df = self.load_and_optimize_fireducks(str(self.dataset_path))
        req = {"event_type", "bytes", "response_time_ms", "risk_score"}
        if not req.issubset(df.columns):
            return None
        return df.groupby("event_type").agg({
            "bytes": ["mean", "min", "max"],
            "response_time_ms": ["mean", "min", "max"],
            "risk_score": ["mean", "min", "max"]
        })
    
    def complex_join_pandas(self, df=None):
        """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
        if df is None:
            df = self.load_and_optimize_pandas(str(self.dataset_path))
        return complex_join_top_ranked(df, rank_col="total_rank", observed=False)
    
    def complex_join_polars(self, df=None):
        """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
        if df is None:
            df = self._read_polars(str(self.dataset_path))
        if not {"source_ip", "bytes", "event_type"}.issubset(set(df.columns)):
            return None
        summary = df.group_by("source_ip").agg(pl.col("bytes").sum().alias("total_bytes"))
        joined = df.join(summary, on="source_ip", how="left")
        ranked = joined.with_columns(
            pl.col("total_bytes").rank("dense", descending=True).over("event_type").alias("total_rank")
        )
        return ranked.filter(pl.col("total_rank") <= 10)
    
    def complex_join_duckdb(self):
        """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
        if not DUCKDB_AVAILABLE:
            return None
        with self.duckdb_source.query(self.dataset_path) as (con, expr):
            # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
            # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
            return con.execute(f"""
                WITH summary AS (
                    SELECT source_ip, SUM(bytes) AS total_bytes
                    FROM {expr}
                    GROUP BY source_ip
                ),
                ranked AS (
                    SELECT d.*, s.total_bytes,
                           DENSE_RANK() OVER (PARTITION BY d.event_type ORDER BY s.total_bytes DESC) AS total_rank
                    FROM {expr} d
                    JOIN summary s USING (source_ip)
                )
                SELECT * FROM ranked WHERE total_rank <= 10
            """).fetch_arrow_table()
    
    def complex_join_fireducks(self, df=None):
        """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
        if df is None:
            df = self.load_and_optimize_fireducks(str(self.dataset_path))
        return complex_join_top_ranked(df, rank_col="total_rank", observed=False)
    
    def timeseries_pandas(self, df=None):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if df is None:
            df = self.load_and_optimize_pandas(str(self.dataset_path))
        return timeseries_hour_counts(df, observed=False, reset_index=False, hour_name="_hour")
    
    def timeseries_polars(self, df=None):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if df is None:
            df = self._read_polars(str(self.dataset_path))
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
    
    def timeseries_duckdb(self):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if not DUCKDB_AVAILABLE:
            return None
        with self.duckdb_source.query(self.dataset_path) as (con, expr):
            # Optimization (2026-05-24): Using fetch_arrow_table() instead of fetchdf()
            # to eliminate severe pandas conversion overhead. DuckDB can output zero-copy PyArrow tables.
            try:
                return con.execute(f"""
                    SELECT EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) AS hour,
                           event_type,
                           COUNT(*) AS count
                    FROM {expr}
                    GROUP BY hour, event_type
                """).fetch_arrow_table()
            except Exception:
                return con.execute(f"""
                    SELECT 0 AS hour, event_type, COUNT(*) AS count
                    FROM {expr}
                    GROUP BY event_type
                """).fetch_arrow_table()
    
    def timeseries_fireducks(self, df=None):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if df is None:
            df = self.load_and_optimize_fireducks(str(self.dataset_path))
        return timeseries_hour_counts(df, observed=False, reset_index=False, hour_name="_hour")
    
    def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """
        Run all benchmark operations across all available libraries with single-load caching.
        
        Returns:
            List of benchmark result dictionaries
        """
        results = []
        host_info = get_host_info()
        operations = ['filter_group', 'statistics', 'complex_join', 'timeseries']
        libraries = [lib for lib, available in self.available_libraries.items() if available]

        print(f"Running {len(operations)} operations across {len(libraries)} libraries...")

        for library in libraries:
            print(f"\nLoading and caching dataset for {library}...")
            cached_df = None
            if library == "pandas":
                cached_df = self.load_and_optimize_pandas(str(self.dataset_path))
            elif library == "fireducks" and FIREDUCKS_AVAILABLE:
                cached_df = self.load_and_optimize_fireducks(str(self.dataset_path))
            elif library == "polars":
                prep_start = time.perf_counter()
                step_start = time.perf_counter()
                cached_df = self._read_polars(str(self.dataset_path))
                _print_prep_timing("polars", "read/load", step_start)
                _print_prep_timing("polars", "total load/optimization", prep_start)
            elif library == "duckdb":
                if self.duckdb_source.mode == "cached":
                    elapsed = self.duckdb_source.prepare(self.dataset_path)
                    record_prep_timing("duckdb", "total load/optimization", elapsed or 0.0)
                    print(f"  [duckdb prep] load cached temp table: {(elapsed or 0.0):.3f}s")
                else:
                    record_prep_timing("duckdb", "total load/optimization", 0.0)
                    print("  [duckdb prep] file scan mode: 0.000s")

            for operation in operations:
                print(f"  Running {operation} with {library}...")
                
                result = self.run_operation(operation, library, cached_df)
                
                # Add metadata
                result.update({
                    'timestamp': time.time(),
                    'operation': operation,
                    'library': library,
                    'dataset_path': str(self.dataset_path),
                    'dataset_size': self.dataset_size,
                    'hostname': host_info.get('hostname', 'unknown'),
                    'cpu_brand': host_info.get('cpu_brand', 'unknown'),
                    'memory_total_gb': host_info.get('memory_total_gb', 0),
                    'python_version': host_info.get('python_version', 'unknown'),
                    'os': host_info.get('platform', 'unknown')
                })

                results.append(result)

                # Print immediate feedback
                if result['status'] == 'success':
                    time_str = f"{result['execution_time']:.4f}s"
                    shape_str = f", shape: {result['result_shape']}" if result['result_shape'] else ""
                    mem_str = f", memory: {result['memory_usage']}MB" if result['memory_usage'] else ""
                    print(f"    ✓ Completed in {time_str}{shape_str}{mem_str}")
                else:
                    print(f"    ✗ {result['status']}: {result['reason']}")
            
            # Release cached DataFrame
            if cached_df is not None:
                print(f"  Releasing cached {library} DataFrame...")
                del cached_df
                step_start = time.perf_counter()
                gc.collect()
                _print_prep_timing(library, "gc after release", step_start)
            elif library == "duckdb":
                self.duckdb_source.close()
        
        return results
    

    def run_operation(self, operation_name: str, library: str, cached_df=None) -> Dict[str, Any]:
        """
        Run a benchmark operation with specified library and cached DataFrame.
        
        Args:
            operation_name: Name of the operation to run
            library: Library to use for the operation
            cached_df: Cached DataFrame (for pandas, polars, fireducks)
            
        Returns:
            Dictionary with benchmark results
        """
        if not self.available_libraries.get(library, False):
            return {
                'status': 'skipped',
                'reason': f'{library} not available',
                'execution_time': None,
                'memory_usage': None,
                'result_shape': None
            }
        
        operation_func = getattr(self, f"{operation_name}_{library}", None)
        if not operation_func:
            return {
                'status': 'error', 
                'reason': f'Operation {operation_name}_{library} not implemented',
                'execution_time': None,
                'memory_usage': None,
                'result_shape': None
            }
        
        try:
            proc = psutil.Process(os.getpid())
            rss_before = proc.memory_info().rss
            start_time = time.perf_counter()
            
            # Execute the operation
            result = operation_func(cached_df) if cached_df is not None else operation_func()
            
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            
            # Get result shape using robust helper function
            result_shape = get_result_shape(result)
            
            # Handle large results by keeping only a small sample
            small_result = None
            if hasattr(result, 'shape') and result.shape and len(result.shape) > 0 and result.shape[0] > 1000:
                try:
                    if hasattr(result, 'head'):
                        small_result = result.head(10)
                    elif hasattr(result, 'limit'):  # Polars
                        small_result = result.limit(10)
                    else:
                        # For other types, try to slice
                        small_result = result[:10] if hasattr(result, '__getitem__') else None
                except Exception:
                    small_result = None
        
            # Replace large result with trimmed version if successful
            if small_result is not None:
                try:
                    del result
                except Exception:
                    pass
                result = small_result
        
            # Calculate memory usage
            rss_after = proc.memory_info().rss
            delta_mb = (rss_after - rss_before) / (1024**2)
            
            # Force garbage collection
            gc.collect()
            
            return {
                'status': 'success',
                'execution_time': execution_time,
                'memory_usage': round(delta_mb, 2),
                'result_shape': result_shape,
                'reason': None
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'reason': str(e),
                'execution_time': None,
                'memory_usage': None,
                'result_shape': None
            }
    
    def save_results(self, results: List[Dict[str, Any]], filename: Optional[str] = None):
        """
        Save benchmark results to CSV file in the same format as original scripts.
        
        Args:
            results: List of benchmark result dictionaries
            filename: Output filename (uses config default if None)
        """
        if not results:
            print("No results to save")
            return
        
        # Convert individual operation results to wide format like original scripts
        operation_results = {}
        for result in results:
            if result['status'] == 'success':
                operation = result['operation']
                library = result['library']
                execution_time = result['execution_time']
                
                if operation not in operation_results:
                    operation_results[operation] = {}
                operation_results[operation][library] = execution_time
        
        # Get host info
        host_info = get_host_info()
        dataset_size = self._get_dataset_size()
        script_name = "benchmark_modular.py"
        
        # Use the original CSV file location (data/benchmark_results.csv)
        original_results_file = self.config.project_root / "data" / "benchmark_results.csv"
        output_file = filename or str(original_results_file)
        
        # Save in original wide format using the same function structure
        self._save_results_to_csv_original_format(operation_results, host_info, script_name, dataset_size, output_file)
    
    def _save_results_to_csv_original_format(self, results: dict, host_info: dict, script_name: str, dataset_size: int, csv_path: Union[str, Path]) -> None:
        """
        Save benchmark results to CSV file in the original format.
        This matches the save_results_to_csv function from the original scripts.
        """
        csv_path = Path(csv_path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
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

            try:
                ds_path = self.dataset_path
                ds_name = ds_path.name
                suffs = [s.lower() for s in ds_path.suffixes]
                comp = {'.gz', '.zip', '.zst', '.bz2'}
                base = [s for s in suffs if s not in comp]
                ext = (base[-1] if base else ds_path.suffix).lower().lstrip('.')
                if ext in ('jsonl', 'ndjson'):
                    ext = 'ndjson'
                ds_fmt = ext or 'unknown'
            except Exception:
                ds_name = 'unknown'
                ds_fmt = 'unknown'

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
                    value = results.get(op, {}).get(lib)
                    row.append("" if value is None else str(value))

            row.extend(get_prep_csv_values())
            row.append(script_name)
            append_csv_row_with_schema(csv_path, header, row)
            print(f"Results saved to: {csv_path}")
            print("  New entry added to benchmark history")
        except Exception as e:
            print(f"Error saving results: {e}")
            raise

def main():
    """Main function with unified CLI parameters (-d/--dataset, -o/--output)."""
    parser = argparse.ArgumentParser(description="Modular Data Processing Benchmark")
    parser.add_argument("-d", "--dataset", type=str, help="Path to dataset file (overrides auto-detect)")
    parser.add_argument("-o", "--output", type=str, help="Results CSV output path (default: data/benchmark_results.csv)")
    parser.add_argument(
        "--optimize",
        "-opt",
        choices=["auto", "always", "never"],
        default="auto",
        help="Memory optimization mode for pandas/fireducks: auto, always, or never (default: auto).",
    )
    parser.add_argument(
        "--mem-threshold",
        "-m",
        type=float,
        default=16.0,
        help="Memory threshold in GB for --optimize auto (default: 16).",
    )
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
        default=None,
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
        help="DuckDB file mode scans per operation; cached mode prevents double-scanning of large files during complex operations (Optimization: 2026-05-24).",
    )
    args = parser.parse_args()
    reset_prep_timings()

    print("="*70)
    print("MODULAR DATA PROCESSING BENCHMARK")
    print("="*70)
    
    # Enhanced host information display with WSL detection
    print("Collecting host information...")
    host_info = get_host_info()
    detector = PlatformDetector()
    
    # Display key system information (matching other benchmark scripts)
    hostname = host_info.get('hostname', 'Unknown')
    system_info = host_info.get('system', 'Unknown')  # Now shows WSL2 instead of Linux
    cpu_brand = host_info.get('cpu_brand', 'Unknown')
    logical_cores = host_info.get('cpu_count_logical', 'N/A')
    memory_total = host_info.get('memory_total_gb', 'N/A')
    
    print(f"Running on: {hostname} ({system_info})")
    print(f"CPU: {cpu_brand} ({logical_cores} logical cores)")
    print(f"Memory: {memory_total} GB total")
    
    #print(f"🔧 Available libraries: {[lib for lib, avail in benchmark.available_libraries.items() if avail]}")
    #print()
    
    # Enhanced library availability display
    available_libs = detector.get_available_benchmark_libraries()
    print(f"Available libraries: {', '.join(available_libs)}")
    cache_dir_display = args.optimized_cache_dir or (PROJECT_ROOT / "data" / "cache" / "optimized")
    print("Preparation settings:")
    print(f"  - Optimization mode: {args.optimize}")
    if args.optimize == "auto":
        print(f"  - Auto optimization threshold: {args.mem_threshold}GB")
    print(f"  - Prep memory report: {args.prep_memory_report}")
    print(f"  - Optimized cache: {args.optimized_cache} ({cache_dir_display})")
    print(f"  - CSV dtype hints: {'disabled' if args.no_csv_dtype_hints else 'enabled'}")
    print(f"  - DuckDB mode: {args.duckdb_mode}")
    print()
    
    try:
        benchmark = ModularBenchmark(
            dataset_path=args.dataset,
            prep_memory_report=args.prep_memory_report,
            optimized_cache_mode=args.optimized_cache,
            optimized_cache_dir=args.optimized_cache_dir,
            use_csv_dtype_hints=not args.no_csv_dtype_hints,
            optimize_mode=args.optimize,
            memory_threshold_gb=args.mem_threshold,
            duckdb_mode=args.duckdb_mode,
        )
        
        print(f"📁 Dataset: {benchmark.dataset_path}")
        print(f"📊 Records: {benchmark.dataset_size:,}")

        # Memory check before starting
        import psutil
        available_memory_gb = psutil.virtual_memory().available / (1024**3)
        if available_memory_gb < 8:
            print(f"Warning: Only {available_memory_gb:.1f}GB available memory. Benchmark may fail.")

        # Run benchmarks
        results = benchmark.run_all_benchmarks()
        
        # Decide output path
        output_override = args.output
        benchmark.save_results(results, filename=output_override)

        print("\n" + "="*70)
        print("BENCHMARK COMPLETED SUCCESSFULLY")
        print("="*70)
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        pass

if __name__ == "__main__":
    main()
