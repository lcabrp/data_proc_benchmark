"""
Modern benchmark script using reusable modular components.

This demonstrates how to build maintainable, reusable code that follows
the DRY principle and can be easily adapted for other projects.
"""

import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from contextlib import redirect_stderr, redirect_stdout

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
    import modin.pandas as mpd
    MODIN_AVAILABLE = True
except ImportError:
    MODIN_AVAILABLE = False

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


class ModularBenchmark:
    """
    Modular benchmark class that demonstrates reusable design patterns.
    
    This class can be easily adapted for different projects by:
    - Changing the operations being benchmarked
    - Adding new data processing libraries
    - Modifying the data sources
    - Customizing the output format
    """
    
    def __init__(self, config=None):
        """Initialize the benchmark with configuration."""
        self.config = config or setup_project()
        self.data_reader = UniversalDataReader(default_library='pandas')
        self.dataset_finder = DatasetFinder(
            search_dirs=self.config.get_dataset_search_dirs(),
            file_patterns=self.config.dataset_patterns
        )
        
        # Find the best dataset
        dataset_path = self.dataset_finder.find_dataset(self.config.project_root)
        if not dataset_path:
            raise FileNotFoundError("No suitable dataset found!")
        
        # Type assertion to help type checker
        self.dataset_path: Path = dataset_path
        print(f"Using dataset: {self.dataset_path}")
        
        # Get dataset size after confirming path is valid
        self.dataset_size = self._get_dataset_size()
        
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
            'modin': MODIN_AVAILABLE,
            'duckdb': DUCKDB_AVAILABLE
        }
    
    def run_operation(self, operation_name: str, library: str) -> Dict[str, Any]:
        """
        Run a benchmark operation with specified library.
        
        Args:
            operation_name: Name of the operation to run
            library: Library to use for the operation
            
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
            start_time = time.time()
            result = operation_func()
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Get result shape if possible
            result_shape = None
            if hasattr(result, 'shape'):
                result_shape = result.shape
            elif hasattr(result, '__len__'):
                result_shape = (len(result),)
            
            return {
                'status': 'success',
                'execution_time': execution_time,
                'memory_usage': None,  # Simplified for now
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
    
    # =====================================================
    # BENCHMARK OPERATIONS - Easily customizable
    # =====================================================
    
    def filter_group_pandas(self):
        """Filter and group operation using pandas."""
        df = self.data_reader.read_file(self.dataset_path, library='pandas')
        return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})
    
    def filter_group_polars(self):
        """Filter and group operation using polars."""
        if not POLARS_AVAILABLE or pl is None:
            raise RuntimeError("Polars not available")
        df = self.data_reader.read_file(self.dataset_path, library='polars')
        return (df.filter(pl.col("status_code") == 200)
                 .group_by("source_ip")
                 .agg(pl.sum("bytes")))
    
    def filter_group_modin(self):
        """Filter and group operation using modin."""
        df = self.data_reader.read_file(self.dataset_path, library='modin')
        return df[df["status_code"] == 200].groupby("source_ip").agg({"bytes": "sum"})
    
    def _duckdb_table_expr(self) -> str:
        """Return a DuckDB table expression for the current dataset path based on format."""
        path = str(self.dataset_path).replace("\\", "/")
        path_l = path.lower()
        if path_l.endswith(".parquet"):
            return f"read_parquet('{path}')"
        else:
            # Fallback to DuckDB's auto CSV reader for csv/ndjson/json and others
            return f"read_csv_auto('{path}')"

    def filter_group_duckdb(self):
        """Filter and group operation using duckdb (SQL)."""
        if not DUCKDB_AVAILABLE:
            raise RuntimeError("DuckDB not available")
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
            return con.execute(
                f"""
                SELECT source_ip, SUM(bytes) AS bytes
                FROM {expr}
                WHERE status_code = 200
                GROUP BY source_ip
                """
            ).fetchdf()
    
    def stats_pandas(self):
        """Statistical analysis using pandas."""
        df = self.data_reader.read_file(self.dataset_path, library='pandas')
        return df.groupby("event_type").agg({
            "bytes": ["mean", "std", "min", "max"],
            "response_time_ms": ["mean", "median"],
            "risk_score": ["mean", "std"]
        })
    
    def stats_polars(self):
        """Statistical analysis using polars."""
        if not POLARS_AVAILABLE or pl is None:
            raise ImportError("Polars is not available")
        
        df = self.data_reader.read_file(self.dataset_path, library='polars')
        return df.group_by("event_type").agg([
            pl.col("bytes").mean().alias("bytes_mean"),
            pl.col("bytes").std().alias("bytes_std"),
            pl.col("bytes").min().alias("bytes_min"),
            pl.col("bytes").max().alias("bytes_max"),
            pl.col("response_time_ms").mean().alias("response_time_ms_mean"),
            pl.col("response_time_ms").median().alias("response_time_ms_median"),
            pl.col("risk_score").mean().alias("risk_score_mean"),
            pl.col("risk_score").std().alias("risk_score_std")
        ])
    
    def stats_modin(self):
        """Statistical analysis using modin."""
        # For parquet files, Modin doesn't support usecols, so read all columns
        if self.dataset_path.suffix == '.parquet':
            df = self.data_reader.read_file(self.dataset_path, library='modin')
        else:
            df = self.data_reader.read_file(self.dataset_path, library='modin', 
                                           usecols=["event_type", "bytes", "response_time_ms", "risk_score"])
        
        # Select only the columns we need for the analysis
        if "event_type" in df.columns and "bytes" in df.columns:
            df = df[["event_type", "bytes", "response_time_ms", "risk_score"]]
        
        return df.groupby("event_type").agg({
            "bytes": ["mean", "std", "min", "max"],
            "response_time_ms": ["mean", "median"], 
            "risk_score": ["mean", "std"]
        })
    
    def stats_duckdb(self):
        """Statistical analysis using duckdb (SQL)."""
        if not DUCKDB_AVAILABLE:
            raise RuntimeError("DuckDB not available")
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
            return con.execute(
                f"""
                SELECT event_type,
                       AVG(bytes) AS bytes_mean,
                       STDDEV(bytes) AS bytes_std,
                       MIN(bytes) AS bytes_min,
                       MAX(bytes) AS bytes_max,
                       AVG(response_time_ms) AS response_time_ms_mean,
                       MEDIAN(response_time_ms) AS response_time_ms_median,
                       AVG(risk_score) AS risk_score_mean,
                       STDDEV(risk_score) AS risk_score_std
                FROM {expr}
                GROUP BY event_type
                """
            ).fetchdf()

    # -------------------------------------------------
    # Complex join/window operations
    # -------------------------------------------------
    def complex_join_pandas(self):
        df = self.data_reader.read_file(self.dataset_path, library='pandas')
        summary = (
            df.groupby("source_ip").agg({
                "bytes": "sum",
                "response_time_ms": "mean",
                "risk_score": "mean"
            }).reset_index()
        )
        summary.columns = [
            "source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"
        ]
        result = df.merge(summary, on="source_ip")
        result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(
            method="dense", ascending=False
        )
        return result[result["bytes_rank"] <= 10]

    def complex_join_polars(self):
        if not POLARS_AVAILABLE or pl is None:
            raise RuntimeError("Polars not available")
        df = self.data_reader.read_file(self.dataset_path, library='polars')
        summary = (
            df.group_by("source_ip").agg([
                pl.col("bytes").sum().alias("total_bytes"),
                pl.col("response_time_ms").mean().alias("avg_response_time_ms"),
                pl.col("risk_score").mean().alias("avg_risk_score"),
            ])
        )
        result = df.join(summary, on="source_ip")
        result = result.with_columns([
            pl.col("bytes").rank(method="dense", descending=True)
            .over("event_type").alias("bytes_rank")
        ])
        return result.filter(pl.col("bytes_rank") <= 10)

    def complex_join_modin(self):
        df = self.data_reader.read_file(self.dataset_path, library='modin')
        summary = (
            df.groupby("source_ip").agg({
                "bytes": "sum",
                "response_time_ms": "mean",
                "risk_score": "mean"
            }).reset_index()
        )
        summary.columns = [
            "source_ip", "total_bytes", "avg_response_time_ms", "avg_risk_score"
        ]
        result = df.merge(summary, on="source_ip")
        result["bytes_rank"] = result.groupby("event_type")["bytes"].rank(
            method="dense", ascending=False
        )
        return result[result["bytes_rank"] <= 10]

    def complex_join_duckdb(self):
        if not DUCKDB_AVAILABLE:
            raise RuntimeError("DuckDB not available")
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
            return con.execute(
                f"""
                WITH summary AS (
                    SELECT source_ip,
                           SUM(bytes) AS total_bytes,
                           AVG(response_time_ms) AS avg_response_time_ms,
                           AVG(risk_score) AS avg_risk_score
                    FROM {expr}
                    GROUP BY source_ip
                ),
                ranked AS (
                    SELECT d.*, s.total_bytes, s.avg_response_time_ms, s.avg_risk_score,
                           DENSE_RANK() OVER (PARTITION BY d.event_type ORDER BY d.bytes DESC) AS bytes_rank
                    FROM {expr} d
                    JOIN summary s ON d.source_ip = s.source_ip
                )
                SELECT * FROM ranked WHERE bytes_rank <= 10
                """
            ).fetchdf()

    # -------------------------------------------------
    # Time series analysis
    # -------------------------------------------------
    def timeseries_pandas(self):
        df = self.data_reader.read_file(self.dataset_path, library='pandas')
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['hour'] = df['timestamp'].dt.hour
            return df.groupby(['hour', 'event_type']).agg({
                'bytes': ['sum', 'count'],
                'response_time_ms': 'mean',
                'risk_score': 'mean'
            })
        else:
            return df.groupby(['status_code', 'event_type']).size().reset_index(name='count')

    def timeseries_modin(self):
        df = self.data_reader.read_file(self.dataset_path, library='modin')
        if 'timestamp' in df.columns:
            import modin.pandas as _mpd
            df['timestamp'] = _mpd.to_datetime(df['timestamp'], errors='coerce')
            df['hour'] = df['timestamp'].dt.hour
            return df.groupby(['hour', 'event_type']).agg({
                'bytes': ['sum', 'count'],
                'response_time_ms': 'mean',
                'risk_score': 'mean'
            })
        else:
            return df.groupby(['status_code', 'event_type']).size().reset_index(name='count')

    def timeseries_polars(self):
        if not POLARS_AVAILABLE or pl is None:
            raise RuntimeError("Polars not available")
        df = self.data_reader.read_file(self.dataset_path, library='polars')
        if 'timestamp' in df.columns:
            df = df.with_columns([
                pl.col('timestamp').str.strptime(pl.Datetime, strict=False).alias('timestamp_parsed'),
            ]).with_columns([
                pl.col('timestamp_parsed').dt.hour().alias('hour')
            ])
            return df.group_by(['hour', 'event_type']).agg([
                pl.col('bytes').sum().alias('bytes_sum'),
                pl.col('bytes').count().alias('bytes_count'),
                pl.col('response_time_ms').mean().alias('response_time_ms_mean'),
                pl.col('risk_score').mean().alias('risk_score_mean'),
            ])
        else:
            return df.group_by(['status_code', 'event_type']).len()

    def timeseries_duckdb(self):
        if not DUCKDB_AVAILABLE:
            raise RuntimeError("DuckDB not available")
        import duckdb as _duckdb
        expr = self._duckdb_table_expr()
        with _duckdb.connect() as con:
            # Try timestamp-based aggregation; fall back if no timestamp
            try:
                return con.execute(
                    f"""
                    SELECT EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) AS hour,
                           event_type,
                           SUM(bytes) AS bytes_sum,
                           COUNT(bytes) AS bytes_count,
                           AVG(response_time_ms) AS response_time_ms_mean,
                           AVG(risk_score) AS risk_score_mean
                    FROM {expr}
                    GROUP BY hour, event_type
                    ORDER BY hour, event_type
                    """
                ).fetchdf()
            except Exception:
                return con.execute(
                    f"""
                    SELECT status_code, event_type, COUNT(*) AS count
                    FROM {expr}
                    GROUP BY status_code, event_type
                    """
                ).fetchdf()
    
    def run_all_benchmarks(self) -> List[Dict[str, Any]]:
        """
        Run all benchmark operations across all available libraries.
        
        Returns:
            List of benchmark result dictionaries
        """
        results = []
        host_info = get_host_info()
        operations = ['filter_group', 'stats', 'complex_join', 'timeseries']
        libraries = [lib for lib, available in self.available_libraries.items() if available]

        print(f"Running {len(operations)} operations across {len(libraries)} libraries...")

        for operation in operations:
            for library in libraries:
                print(f"  Running {operation} with {library}...")

                result = self.run_operation(operation, library)

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
                    print(f"    ✓ Completed in {time_str}{shape_str}")
                else:
                    print(f"    ✗ {result['status']}: {result['reason']}")

        return results
    
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
                        "dataset_size",  # Number of records in the dataset
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
                
                # Helper function to handle None values (match original: use 'N/A')
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
                    dataset_size,  # Dataset size
                    # Timing columns (fixed order: operation first, then library)
                    safe_value(results.get("filter_group", {}).get("pandas")),
                    safe_value(results.get("filter_group", {}).get("modin")),
                    safe_value(results.get("filter_group", {}).get("polars")),
                    safe_value(results.get("filter_group", {}).get("duckdb")),
                    safe_value(results.get("filter_group", {}).get("fireducks")),
                    safe_value(results.get("stats", {}).get("pandas")),  # Note: "stats" not "statistics"
                    safe_value(results.get("stats", {}).get("modin")),
                    safe_value(results.get("stats", {}).get("polars")),
                    safe_value(results.get("stats", {}).get("duckdb")),
                    safe_value(results.get("stats", {}).get("fireducks")),
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
            print(f"✓ Results saved to: {csv_path}")
            print(f"  New entry added to benchmark history")
        except Exception as e:
            print(f"Error saving results: {e}")
            raise


def main():
    """Main function demonstrating the modular benchmark system."""
    print("="*70)
    print("MODULAR DATA PROCESSING BENCHMARK")
    print("="*70)
    
    try:
        # Initialize benchmark with automatic configuration
        benchmark = ModularBenchmark()
        
        print(f"📁 Dataset: {benchmark.dataset_path}")
        print(f"📊 Records: {benchmark.dataset_size:,}")
        print(f"🔧 Available libraries: {[lib for lib, avail in benchmark.available_libraries.items() if avail]}")
        print()
        
        # Run benchmarks
        results = benchmark.run_all_benchmarks()
        
        # Save results
        benchmark.save_results(results)
        
        print("\n" + "="*70)
        print("BENCHMARK COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
