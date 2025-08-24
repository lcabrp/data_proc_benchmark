"""
Platform detection and library availability utilities.

Provides centralized platform detection, library availability checking,
and environment-specific configuration recommendations for benchmarking.
"""

import platform
import warnings
from typing import Dict, Any, Optional


class PlatformDetector:
    """Centralized platform detection and configuration utilities."""
    
    @staticmethod
    def get_platform_flags() -> Dict[str, bool]:
        """
        Get platform detection flags.
        
        Returns:
            dict: Platform flags (IS_WINDOWS, IS_WSL, IS_LINUX, IS_MACOS)
        """
        uname_release = platform.uname().release
        system = platform.system()
        
        return {
            'IS_WINDOWS': system == 'Windows',
            'IS_WSL': 'WSL' in uname_release,
            'IS_LINUX': system == 'Linux' and 'WSL' not in uname_release,
            'IS_MACOS': system == 'Darwin'
        }
    
    @staticmethod
    def check_library_availability() -> Dict[str, bool]:
        """
        Check availability of optional libraries.
        
        Returns:
            dict: Library availability flags
        """
        libraries = {}
        
        # Check Ray availability (all platforms)
        try:
            import ray
            libraries['RAY_AVAILABLE'] = True
        except ImportError:
            libraries['RAY_AVAILABLE'] = False
        
        # Check FireDucks availability (Linux/macOS/WSL)
        platforms = PlatformDetector.get_platform_flags()
        if not platforms['IS_WINDOWS'] or platforms['IS_WSL']:
            try:
                import fireducks.pandas as fpd
                libraries['FIREDUCKS_AVAILABLE'] = True
            except ImportError:
                libraries['FIREDUCKS_AVAILABLE'] = False
        else:
            libraries['FIREDUCKS_AVAILABLE'] = False
        
        # Check other optional libraries
        optional_libs = ['cpuinfo', 'wmi', 'GPUtil']
        for lib in optional_libs:
            try:
                __import__(lib)
                libraries[f'{lib.upper()}_AVAILABLE'] = True
            except ImportError:
                libraries[f'{lib.upper()}_AVAILABLE'] = False
        
        return libraries
    
    @staticmethod
    def get_recommended_modin_engine() -> str:
        """
        Get recommended Modin engine based on platform and available libraries.
        
        Returns:
            str: Recommended engine ('ray' or 'dask')
        """
        platforms = PlatformDetector.get_platform_flags()
        libraries = PlatformDetector.check_library_availability()
        
        # Windows: prefer Dask (more stable)
        if platforms['IS_WINDOWS']:
            return "dask"
        
        # Linux/macOS/WSL: prefer Ray if available, fallback to Dask
        return "ray" if libraries['RAY_AVAILABLE'] else "dask"
    
    @staticmethod
    def get_dask_cluster_config() -> Dict[str, Any]:
        """
        Get recommended Dask cluster configuration based on platform.
        
        Returns:
            dict: Cluster configuration parameters
        """
        import psutil
        
        platforms = PlatformDetector.get_platform_flags()
        total_memory = psutil.virtual_memory().total
        total_gb = total_memory // (1024 ** 3)
        logical_cores = psutil.cpu_count(logical=True) or 4
        
        if platforms['IS_WINDOWS']:
            # Windows: threaded workers for stability
            return {
                'n_workers': min(2, max(1, logical_cores)),
                'threads_per_worker': max(1, logical_cores // min(2, logical_cores)),
                'processes': False,
                'memory_target_gb_per_worker': total_gb // 2
            }
        else:
            # Linux/macOS: process-based workers
            target_per_worker_gb = 4
            n_workers_by_mem = max(1, int(total_gb // target_per_worker_gb))
            n_workers = min(logical_cores, n_workers_by_mem)
            
            return {
                'n_workers': max(1, n_workers),
                'threads_per_worker': max(1, logical_cores // n_workers),
                'processes': True,
                'memory_target_gb_per_worker': target_per_worker_gb
            }
    
    @staticmethod
    def setup_ray_if_needed(engine: str) -> bool:
        """
        Initialize Ray if using Ray engine.
        
        Args:
            engine (str): The engine being used ('ray' or 'dask')
            
        Returns:
            bool: True if Ray was successfully initialized or not needed
        """
        if engine != "ray":
            return True
        
        libraries = PlatformDetector.check_library_availability()
        if not libraries['RAY_AVAILABLE']:
            return False
        
        try:
            import ray
            if not ray.is_initialized():
                ray.init(ignore_reinit_error=True)
                print("Ray initialized for Modin")
            return True
        except Exception as e:
            print(f"Warning: Failed to initialize Ray: {e}")
            return False
    
    @staticmethod
    def cleanup_ray_if_needed(engine: str) -> None:
        """
        Cleanup Ray if it was used.
        
        Args:
            engine (str): The engine that was used ('ray' or 'dask')
        """
        if engine != "ray":
            return
        
        libraries = PlatformDetector.check_library_availability()
        if not libraries['RAY_AVAILABLE']:
            return
        
        try:
            import ray
            if ray.is_initialized():
                ray.shutdown()
                print("Ray shutdown completed")
        except Exception as e:
            print(f"Warning: Ray shutdown failed: {e}")
    
    @staticmethod
    def get_available_benchmark_libraries() -> list:
        """
        Get list of available libraries for benchmarking.
        
        Returns:
            list: List of available library names
        """
        libraries = ["pandas", "modin", "polars", "duckdb"]
        availability = PlatformDetector.check_library_availability()
        
        if availability['FIREDUCKS_AVAILABLE']:
            libraries.append("fireducks")
        
        return libraries
    
    @staticmethod
    def print_environment_info() -> None:
        """Print comprehensive environment information."""
        platforms = PlatformDetector.get_platform_flags()
        libraries = PlatformDetector.check_library_availability()
        
        print("=== Environment Information ===")
        print(f"Platform: {platform.system()} {platform.release()}")
        print(f"Architecture: {platform.machine()}")
        print(f"Python: {platform.python_version()}")
        
        print("\n=== Platform Flags ===")
        for flag, value in platforms.items():
            print(f"{flag}: {value}")
        
        print("\n=== Library Availability ===")
        for lib, available in libraries.items():
            status = "✓" if available else "✗"
            print(f"{status} {lib}")
        
        print(f"\n=== Recommended Modin Engine ===")
        print(f"Engine: {PlatformDetector.get_recommended_modin_engine()}")


# Backward compatibility - expose functions directly
def get_platform_flags():
    return PlatformDetector.get_platform_flags()

def check_library_availability():
    return PlatformDetector.check_library_availability()

def get_recommended_modin_engine():
    return PlatformDetector.get_recommended_modin_engine()
