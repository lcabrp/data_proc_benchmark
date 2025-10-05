"""
Modern benchmark script using reusable modular components.

This demonstrates how to build maintainable, reusable code that follows
the DRY principle and can be easily adapted for other projects.
"""

import sys
import time
import gc
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, cast
from contextlib import redirect_stderr, redirect_stdout
import argparse
import platform

import warnings
import psutil
warnings.filterwarnings("ignore", category=SyntaxWarning, message=r"invalid escape sequence \\_")

# Add project root to Python path so we can import utils modules
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Setup environment and import reusable modules
from utils.config import setup_project
from utils.data_io import UniversalDataReader, DatasetFinder
from utils.host_info import get_host_info

# Import data processing libraries
import pandas as pd
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

import numpy as np

# FireDucks check (Linux/macOS only)
FIREDUCKS_AVAILABLE = False
if platform.system() in ['Linux', 'Darwin']:
    try:
        import fireducks.pandas as fpd
        FIREDUCKS_AVAILABLE = True
    except ImportError:
        pass

from utils import optimize_df_types

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
    
    def __init__(self, config=None, dataset_path: Optional[Union[str, Path]] = None):
        """Initialize the benchmark with configuration.

        Args:
            config: Optional pre-built project config.
            dataset_path: Optional explicit dataset path override (CLI -d/--dataset).
        """
        self.config = config or setup_project()
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
            'pandas': True,  # Always available as it's required
            'polars': POLARS_AVAILABLE,
            'duckdb': DUCKDB_AVAILABLE,
            'fireducks': FIREDUCKS_AVAILABLE
        }
    
    # Optimization for pandas/fireducks
    def optimize_benchmark_df(self, bdf: pd.DataFrame) -> pd.DataFrame:
        """
        Apply custom dtype optimization rules for memory efficiency.
        Matches benchmark.py optimizations.
        """
        new_dict_types = {
            'datetime64[ns]': ['timestamp'],
            'category': ['source_ip', 'destination_ip', 'protocol', 'event_type',
                         'severity', 'user', 'status_code', 'country', 'device_type'],
            'uint32': ['bytes', 'session_id'],
            'uint16': ['response_time_ms', 'port'],
            'float32': ['risk_score']
        }
        return optimize_df_types(bdf, new_dict_types)

    def load_and_optimize_pandas(self, csv_path: str) -> pd.DataFrame:
        """
        Load and optimize pandas DataFrame once.
        """
        if csv_path.endswith('.parquet'):
            df = pd.read_parquet(csv_path)
        else:
            df = pd.read_csv(csv_path)
        
        try:
            # Use df_memory_usage for detailed memory tracking
            from utils.useful_functions import df_memory_usage
            original_mem = df_memory_usage(df)
            original_memory = original_mem['Total']

            print(f"  Original memory usage: {original_memory/1024/1024:.1f}MB")
            
            opt = self.optimize_benchmark_df(df)
            
            optimized_mem = df_memory_usage(opt)
            optimized_memory = optimized_mem['Total']
            print(f"  Optimized memory usage: {optimized_memory/1024/1024:.1f}MB")
            
            memory_reduction = (original_memory - optimized_memory) / original_memory * 100
            print(f"  pandas DataFrame optimized: {memory_reduction:.1f}% memory reduction ({original_memory/1024/1024:.1f}MB → {optimized_memory/1024/1024:.1f}MB)")
            del df
            gc.collect()
            return opt
        except Exception as e:
            print(f"Warning: Optimization failed: {e}")
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
        if csv_path.endswith('.parquet'):
            df = fpd.read_parquet(csv_path)
        else:
            df = fpd.read_csv(csv_path)
        try:
            from utils.useful_functions import df_memory_usage
            original = df_memory_usage(df)
            opt = self.optimize_benchmark_df(df)
            optimized = df_memory_usage(opt)
            red = (original['Total'] - optimized['Total']) / original['Total'] * 100 if original['Total'] > 0 else 0
            print(f"  fireducks DataFrame optimized: {red:.1f}% memory reduction ({original['Total']/1024/1024:.1f}MB → {optimized['Total']/1024/1024:.1f}MB)")
            del df
            gc.collect()
            return opt
        except Exception as e:
            print(f"Warning: FireDucks optimization failed: {e}")
            return df

    def _duckdb_table_expr(self):
        """Generate DuckDB table expression for the dataset."""
        reader = UniversalDataReader()
        fmt = reader.detect_file_format(self.dataset_path)
        if fmt == 'parquet':
            return f"read_parquet('{self.dataset_path}')"
        else:
            return f"read_csv_auto('{self.dataset_path}')"

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
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
            return con.execute(f"""
                SELECT event_type, COUNT(*) AS count
                FROM {expr}
                WHERE bytes > 1000
                GROUP BY event_type
            """).fetchdf()
    
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
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
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
            """).fetchdf()
    
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
        req = {"source_ip", "bytes", "event_type"}
        if not req.issubset(df.columns):
            return None
        summary = df.groupby("source_ip", observed=False)["bytes"].sum().reset_index().rename(columns={"bytes": "total_bytes"})
        merged = df.merge(summary, on="source_ip", how="left")
        merged["total_rank"] = merged.groupby("event_type", observed=False)["total_bytes"].rank(method="dense", ascending=False)
        return merged.loc[merged["total_rank"] <= 10]
    
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
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
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
            """).fetchdf()
    
    def complex_join_fireducks(self, df=None):
        """Sum bytes by source_ip, join back, rank by total_bytes per event_type, top 10."""
        if df is None:
            df = self.load_and_optimize_fireducks(str(self.dataset_path))
        req = {"source_ip", "bytes", "event_type"}
        if not req.issubset(df.columns):
            return None
        summary = df.groupby("source_ip")["bytes"].sum().reset_index().rename(columns={"bytes": "total_bytes"})
        merged = df.merge(summary, on="source_ip", how="left")
        merged["total_rank"] = merged.groupby("event_type")["total_bytes"].rank(method="dense", ascending=False)
        return merged[merged["total_rank"] <= 10]
    
    def timeseries_pandas(self, df=None):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if df is None:
            df = self.load_and_optimize_pandas(str(self.dataset_path))
        if "event_type" not in df.columns:
            return None
        work = df.copy()
        if "timestamp" in work.columns:
            ts = pd.to_datetime(work["timestamp"], errors="coerce")
            hour = ts.dt.hour
        else:
            hour = 0
        work = work.assign(_hour=hour)
        return work.groupby(["_hour", "event_type"], observed=False).size()
    
    def timeseries_polars(self, df=None):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if df is None:
            df = self._read_polars(str(self.dataset_path))
        if "event_type" not in df.columns:
            return None
        if "timestamp" in df.columns:
            df2 = df.with_columns(
                pl.col("timestamp").str.to_datetime(strict=False).dt.hour().alias("_hour")
            )
        else:
            df2 = df.with_columns(pl.lit(0).alias("_hour"))
        return df2.group_by(["_hour", "event_type"]).agg(pl.len().alias("count"))
    
    def timeseries_duckdb(self):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if not DUCKDB_AVAILABLE:
            return None
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
            try:
                return con.execute(f"""
                    SELECT EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) AS hour,
                           event_type,
                           COUNT(*) AS count
                    FROM {expr}
                    GROUP BY hour, event_type
                """).fetchdf()
            except Exception:
                return con.execute(f"""
                    SELECT 0 AS hour, event_type, COUNT(*) AS count
                    FROM {expr}
                    GROUP BY event_type
                """).fetchdf()
    
    def timeseries_fireducks(self, df=None):
        """Extract hour from timestamp, group by (hour, event_type), count."""
        if df is None:
            df = self.load_and_optimize_fireducks(str(self.dataset_path))
        if "event_type" not in df.columns:
            return None
        if "timestamp" in df.columns:
            ts = pd.to_datetime(df["timestamp"], errors="coerce")
            hour = ts.dt.hour
        else:
            hour = 0
        tmp = df.assign(_hour=hour)
        return tmp.groupby(["_hour", "event_type"]).size()
    
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
                cached_df = self._read_polars(str(self.dataset_path))

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
                    time_str = f"{result['execution_time']:.2f}s"
                    shape_str = f", shape: {result['result_shape']}" if result['result_shape'] else ""
                    mem_str = f", memory: {result['memory_usage']}MB" if result['memory_usage'] else ""
                    print(f"    ✓ Completed in {time_str}{shape_str}{mem_str}")
                else:
                    print(f"    ✗ {result['status']}: {result['reason']}")
            
            # Release cached DataFrame
            if cached_df is not None:
                print(f"  Releasing cached {library} DataFrame...")
                del cached_df
                gc.collect()
        
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
        import csv
        from pathlib import Path
        
        csv_path = Path(csv_path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            file_exists = csv_path.exists()
            with open(csv_path, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                if not file_exists:
                    header = [
                        "timestamp", "hostname", "platform", "system", "release", "version", "machine", "processor",
                        "cpu_count_logical", "cpu_count_physical", "cpu_freq_max", "cpu_freq_current",
                        "memory_total_gb", "memory_available_gb", "python_version", "python_implementation",
                        "cpu_brand", "cpu_arch",  # Host info ends here
                        "dataset_size", "dataset_name", "dataset_format",
                        # Removed Modin columns; now matches other modules
                        "filter_group_pandas_seconds", "filter_group_polars_seconds",
                        "filter_group_duckdb_seconds", "filter_group_fireducks_seconds",
                        "statistics_pandas_seconds", "statistics_polars_seconds",
                        "statistics_duckdb_seconds", "statistics_fireducks_seconds",
                        "complex_join_pandas_seconds", "complex_join_polars_seconds",
                        "complex_join_duckdb_seconds", "complex_join_fireducks_seconds",
                        "timeseries_pandas_seconds", "timeseries_polars_seconds",
                        "timeseries_duckdb_seconds", "timeseries_fireducks_seconds",
                        "script_name"  # Moved to the end
                    ]
                    writer.writerow(header)
                
                # Dataset metadata
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
                    # Timing columns (removed Modin; use full precision without truncation)
                    "" if results.get("filter_group", {}).get("pandas") is None else str(results.get("filter_group", {}).get("pandas")),
                    "" if results.get("filter_group", {}).get("polars") is None else str(results.get("filter_group", {}).get("polars")),
                    "" if results.get("filter_group", {}).get("duckdb") is None else str(results.get("filter_group", {}).get("duckdb")),
                    "" if results.get("filter_group", {}).get("fireducks") is None else str(results.get("filter_group", {}).get("fireducks")),
                    "" if results.get("statistics", {}).get("pandas") is None else str(results.get("statistics", {}).get("pandas")),
                    "" if results.get("statistics", {}).get("polars") is None else str(results.get("statistics", {}).get("polars")),
                    "" if results.get("statistics", {}).get("duckdb") is None else str(results.get("statistics", {}).get("duckdb")),
                    "" if results.get("statistics", {}).get("fireducks") is None else str(results.get("statistics", {}).get("fireducks")),
                    "" if results.get("complex_join", {}).get("pandas") is None else str(results.get("complex_join", {}).get("pandas")),
                    "" if results.get("complex_join", {}).get("polars") is None else str(results.get("complex_join", {}).get("polars")),
                    "" if results.get("complex_join", {}).get("duckdb") is None else str(results.get("complex_join", {}).get("duckdb")),
                    "" if results.get("complex_join", {}).get("fireducks") is None else str(results.get("complex_join", {}).get("fireducks")),
                    "" if results.get("timeseries", {}).get("pandas") is None else str(results.get("timeseries", {}).get("pandas")),
                    "" if results.get("timeseries", {}).get("polars") is None else str(results.get("timeseries", {}).get("polars")),
                    "" if results.get("timeseries", {}).get("duckdb") is None else str(results.get("timeseries", {}).get("duckdb")),
                    "" if results.get("timeseries", {}).get("fireducks") is None else str(results.get("timeseries", {}).get("fireducks")),
                    script_name  # Moved to the end
                ]
                writer.writerow(row)
                print(f"✓ Results saved to: {csv_path}")
                print(f"  New entry added to benchmark history")
        except Exception as e:
            print(f"Error saving results: {e}")
            raise


def run_operation(library, operation_name, operation_func):
    """Run a single operation for a library with timing and error handling."""
    # Removed Modin-specific handling
    try:
        if library == "fireducks" and not FIREDUCKS_AVAILABLE:
            print(f"  Running {operation_name} with {library}...")
            print(f"    ✗ skipped: FireDucks not available")
            return None, None
        
        print(f"  Running {operation_name} with {library}...")
        start_time = time.time()
        
        result = operation_func()
        
        duration = time.time() - start_time
        shape = getattr(result, 'shape', 'N/A')
        print(f"    ✓ Completed in {duration:.2f}s, shape: {shape}")
        return duration, result
    except Exception as e:
        print(f"    ✗ error: {e}")
        return None, None


def run_benchmarks():
    """Run all benchmarks for available libraries and operations."""
    # Removed Modin from libraries
    libraries = ['pandas', 'polars', 'duckdb']
    operations = ['filter_group', 'stats', 'complex_join', 'timeseries']
    
    results = {}
    for operation in operations:
        print(f"  Running {operation} with all libraries...")
        results[operation] = {}
        for library in libraries:
            func_name = f"{library}_{operation}"
            if func_name in globals():
                duration, _ = run_operation(library, operation, globals()[func_name])
                results[operation][library] = duration
            else:
                print(f"    Warning: {func_name} not found, skipping.")
                results[operation][library] = None
    
    return results


def main():
    """Main function with unified CLI parameters (-d/--dataset, -o/--output)."""
    parser = argparse.ArgumentParser(description="Modular Data Processing Benchmark")
    parser.add_argument("-d", "--dataset", type=str, help="Path to dataset file (overrides auto-detect)")
    parser.add_argument("-o", "--output", type=str, help="Results CSV output path (default: data/benchmark_results.csv)")
    args = parser.parse_args()

    print("="*70)
    print("MODULAR DATA PROCESSING BENCHMARK")
    print("="*70)

    try:
        benchmark = ModularBenchmark(dataset_path=args.dataset)
        
        print(f"📁 Dataset: {benchmark.dataset_path}")
        print(f"📊 Records: {benchmark.dataset_size:,}")
        print(f"🔧 Available libraries: {[lib for lib, avail in benchmark.available_libraries.items() if avail]}")
        print()

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
