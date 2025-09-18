"""
Data Processing Benchmark Utilities

This package provides reusable utilities for system information collection,
platform detection, memory monitoring, benchmark configuration, and additional helper functions.
"""

from .host_info import get_host_info, SystemInfo
from .memory_utils import get_memory_usage_mb, log_memory_usage
from .platform_utils import PlatformDetector
from .useful_functions import *  # Import all functions from useful_functions.py

__all__ = [
    'get_host_info',
    'SystemInfo', 
    'get_memory_usage_mb',
    'log_memory_usage',
    'PlatformDetector'
    ,'to_plain_pandas',
    'get_files_dir',
    'show_df_info',
    'optimize_types',
    'auto_opt_pd_dtypes',
    'get_types',
    'downcast_int',
    'analyze_dataframe_for_optimization'  # Add any other functions from useful_functions.py here
]
