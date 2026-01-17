"""
Host-to-Host Benchmark Comparison Tool

This module provides functionality to compare benchmark results between two hosts,
with statistical outlier detection and detailed performance analysis.

Key Features:
- Compare performance metrics between two hosts
- Automatic outlier removal using IQR method (1.5x IQR threshold)
- Per-library and per-operation analysis (pandas, polars, duckdb, fireducks)
- OS-level comparisons (Windows, WSL2, Linux)
- Dataset format comparisons (CSV, Parquet)
- Caching mechanism to avoid redundant computations
- JSON/NDJSON export for integration with other tools

Usage:
    python scripts/tools/compare_hosts.py \\
        --csv data/benchmark_results.csv \\
        --host Host1 \\
        --host Host2 \\
        [--keep-outliers] \\
        [--quiet]

Author: Data Processing Benchmark Project
License: MIT
"""

import csv
import argparse
import sys
from pathlib import Path
from statistics import mean, median, stdev
from typing import Dict, List, Optional, Tuple
import difflib
from datetime import datetime, timezone
import os
import numpy as np

# Mapping of library names to their CSV column names
LIB_OPS: Dict[str, List[str]] = {
    'pandas': [
        'filter_group_pandas_seconds',
        'statistics_pandas_seconds',
        'complex_join_pandas_seconds',
        'timeseries_pandas_seconds',
    ],
    'polars': [
        'filter_group_polars_seconds',
        'statistics_polars_seconds',
        'complex_join_polars_seconds',
        'timeseries_polars_seconds',
    ],
    'duckdb': [
        'filter_group_duckdb_seconds',
        'statistics_duckdb_seconds',
        'complex_join_duckdb_seconds',
        'timeseries_duckdb_seconds',
    ],
    'fireducks': [
        'filter_group_fireducks_seconds',
        'statistics_fireducks_seconds',
        'complex_join_fireducks_seconds',
        'timeseries_fireducks_seconds',
    ],
}


def fval(x: Optional[str]) -> Optional[float]:
    """
    Convert a CSV field value to float, handling None/empty/N/A cases.
    
    Args:
        x: String value from CSV field
        
    Returns:
        Float value if conversion successful, None otherwise
        
    Examples:
        >>> fval("3.14")
        3.14
        >>> fval("N/A")
        None
        >>> fval("")
        None
    """
    try:
        return float(x) if x not in (None, "", "N/A") else None
    except Exception:
        return None


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    """
    Load all rows from the benchmark results CSV file.
    
    Args:
        csv_path: Path to the benchmark results CSV file
        
    Returns:
        List of dictionaries, one per row, with column names as keys
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        PermissionError: If CSV file can't be read
    """
    with open(csv_path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        return list(rdr)
