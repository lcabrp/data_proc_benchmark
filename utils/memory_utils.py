"""
Memory monitoring utilities for data processing benchmarks.

Provides functions to track memory usage of the current process and its children,
useful for monitoring memory consumption during benchmark operations.
"""

import os
import psutil
from typing import Optional


def get_memory_usage_mb() -> float:
    """
    Get memory usage of the current process and its children.
    
    Returns:
        float: Memory usage in MB.
    """
    try:
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss
        for child in process.children(recursive=True):
            try:
                mem += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # Child process may have terminated
                pass
        return mem / (1024 ** 2)
    except Exception as e:
        print(f"Warning: Could not get memory usage: {e}")
        return 0.0


def log_memory_usage(label: str = "") -> None:
    """
    Log current memory usage (including child processes).
    
    Args:
        label (str): Optional label for the log.
    """
    mem_usage_mb = get_memory_usage_mb()
    print(f"[{label}] Memory usage: {mem_usage_mb:.2f} MB")


def get_system_memory_info() -> dict:
    """
    Get system-wide memory information.
    
    Returns:
        dict: Memory information including total, available, used, etc.
    """
    try:
        vm = psutil.virtual_memory()
        return {
            'total_gb': round(vm.total / (1024**3), 2),
            'available_gb': round(vm.available / (1024**3), 2),
            'used_gb': round(vm.used / (1024**3), 2),
            'percent_used': vm.percent,
            'free_gb': round(vm.free / (1024**3), 2) if hasattr(vm, 'free') else None
        }
    except Exception as e:
        return {'error': f'Failed to get memory info: {e}'}


def check_memory_pressure(threshold_percent: float = 85.0) -> bool:
    """
    Check if system memory usage is above a threshold.
    
    Args:
        threshold_percent (float): Memory usage percentage threshold (default: 85%)
        
    Returns:
        bool: True if memory usage is above threshold
    """
    try:
        return psutil.virtual_memory().percent > threshold_percent
    except Exception:
        return False
