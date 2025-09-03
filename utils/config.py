"""
Configuration module for reusable project settings.

This module provides standardized configuration patterns that can be reused
across different data processing projects.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
import os


class ProjectConfig:
    """
    Base configuration class for data processing projects.
    
    Design Philosophy:
    - Environment-aware: automatically detect project structure
    - Flexible: easily customizable for different projects
    - Standardized: consistent naming and organization
    - Portable: works across different operating systems
    """
    
    def __init__(self, project_root: Optional[Path] = None):
        """
        Initialize project configuration.
        
        Args:
            project_root: Root directory of the project (auto-detected if None)
        """
        self.project_root = self._find_project_root(project_root)
        self.data_dir = self.project_root / "data"
        self.results_dir = self.data_dir / "results"
        self.raw_data_dir = self.data_dir / "raw"
        self.processed_data_dir = self.data_dir / "processed"
        self.scripts_dir = self.project_root / "scripts"
        self.utils_dir = self.project_root / "utils"
        
        # Ensure directories exist
        self._create_directories()
    
    def _find_project_root(self, project_root: Optional[Path]) -> Path:
        """Find the project root directory."""
        if project_root:
            return Path(project_root).resolve()
        
        # Try to find project root by looking for common markers
        current_dir = Path(__file__).parent
        
        # Look for common project markers
        markers = [
            'pyproject.toml',
            'requirements.txt',
            'setup.py',
            '.git',
            'README.md'
        ]
        
        # Search up the directory tree
        for parent in [current_dir] + list(current_dir.parents):
            if any((parent / marker).exists() for marker in markers):
                return parent
        
        # Fallback to current directory's parent
        return current_dir.parent
    
    def _create_directories(self):
        """Create necessary directories if they don't exist."""
        directories = [
            self.data_dir,
            self.results_dir,
            self.raw_data_dir,
            self.processed_data_dir,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_results_file(self, filename: str = "benchmark_results.csv") -> Path:
        """Get path to results file."""
        return self.results_dir / filename
    
    def get_raw_data_file(self, filename: str) -> Path:
        """Get path to raw data file."""
        return self.raw_data_dir / filename
    
    def get_processed_data_file(self, filename: str) -> Path:
        """Get path to processed data file."""
        return self.processed_data_dir / filename


class BenchmarkConfig(ProjectConfig):
    """
    Configuration specifically for benchmarking projects.
    
    Extends ProjectConfig with benchmark-specific settings.
    """
    
    def __init__(self, project_root: Optional[Path] = None):
        super().__init__(project_root)

        # Benchmark-specific settings
        # Default results file: place at data/benchmark_results.csv for visibility
        # (can still be overridden via CLI --output in scripts)
        self.benchmark_results_file = self.data_dir / "benchmark_results.csv"

        # Default dataset search patterns (ordered by preference)
        self.dataset_patterns = [
            "*_7M.parquet", "*_7M.csv",
            "*_10M.parquet", "*_10M.csv",
            "*_5M.parquet", "*_5M.csv",
            "*_1M.parquet", "*_1M.csv",
            "synthetic_logs*.parquet", "synthetic_logs*.csv",
            "*.parquet", "*.csv",
            "*.ndjson", "*.jsonl", "*.json",
            "*.csv.gz", "*.parquet.gz",
        ]

        # Libraries to test (in order of preference)
        self.benchmark_libraries = [
            "pandas",
            "polars",
            "modin",
            "duckdb",
        ]

        # Operations to benchmark
        self.benchmark_operations = [
            "filter_group",
            "stats",
            "join",
            "window",
        ]
    
    def get_dataset_search_dirs(self) -> List[Path]:
        """Get directories to search for datasets."""
        return [
            self.raw_data_dir,
            self.data_dir,
            self.project_root / "datasets",
            self.project_root
        ]


class EnvironmentConfig:
    """
    Environment configuration for consistent setup across projects.
    """
    
    @staticmethod
    def setup_pandas_options():
        """Setup standard pandas display options."""
        try:
            import pandas as pd
            pd.set_option('display.max_columns', 20)
            pd.set_option('display.max_rows', 100)
            pd.set_option('display.width', 1000)
            pd.set_option('display.float_format', '{:.2f}'.format)
        except ImportError:
            pass
    
    @staticmethod
    def setup_warnings():
        """Setup standard warning filters."""
        import warnings
        warnings.filterwarnings("ignore", category=FutureWarning, module="modin")
        warnings.filterwarnings("ignore", category=UserWarning, module="ray")
        warnings.filterwarnings("ignore", category=UserWarning, module="dask")
    
    @staticmethod
    def add_project_to_path(project_root: Optional[Path] = None):
        """Add project root to Python path for imports."""
        import sys
        if project_root is None:
            # Auto-detect project root
            project_root = Path(__file__).parent.parent
        
        project_root = Path(project_root).resolve()
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
    
    @classmethod
    def setup_environment(cls, project_root: Optional[Path] = None):
        """Setup complete environment for data processing."""
        cls.add_project_to_path(project_root)
        cls.setup_pandas_options()
        cls.setup_warnings()


# Convenience function for quick setup
def setup_project(project_root: Optional[Path] = None) -> BenchmarkConfig:
    """
    Quick setup function for data processing projects.
    
    Args:
        project_root: Root directory of the project
        
    Returns:
        Configured BenchmarkConfig instance
    """
    EnvironmentConfig.setup_environment(project_root)
    return BenchmarkConfig(project_root)
