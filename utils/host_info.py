"""
Host Information Collection Utilities

This module provides utilities for collecting system information across different platforms.
Optimized for reliable operation without blocking WMI calls on Windows.
Enhanced with WSL detection and platform-aware system identification.
Logging is disabled to prevent verbosity in benchmarks.
"""

import datetime
import platform
import socket
import psutil
from typing import Dict, Any, Optional

# Disable logging to prevent verbosity
import logging
logging.disable(logging.CRITICAL)  # Disable all logging from this module

def get_host_info() -> Dict[str, Any]:
    """
    Collect comprehensive host system information with enhanced platform detection.
    
    Enhanced Features:
    - WSL detection (WSL1, WSL2, future WSL3)
    - Accurate system categorization for analysis
    - Platform-aware information collection
    
    Returns:
        Dict[str, Any]: Host system information with enhanced platform detection
    """
    try:
        # Import platform detector for enhanced system identification
        from .platform_utils import PlatformDetector
        detector = PlatformDetector()
        
        # Basic system information
        info = {
            'timestamp': datetime.datetime.now().isoformat(),
            'hostname': socket.gethostname(),
            'platform': platform.platform(),
            'system': _get_enhanced_system_name(detector),
            'release': platform.release(),
            'version': platform.version(),
            'machine': platform.machine(),
            'processor': platform.processor(),
            'cpu_count_logical': psutil.cpu_count(logical=True),
            'cpu_count_physical': psutil.cpu_count(logical=False),
        }
        
        # Add CPU frequency if available
        try:
            cpu_freq = psutil.cpu_freq()
            if cpu_freq:
                info['cpu_freq_max'] = cpu_freq.max
                info['cpu_freq_current'] = cpu_freq.current
            else:
                info['cpu_freq_max'] = 'N/A'
                info['cpu_freq_current'] = 'N/A'
        except Exception:
            info['cpu_freq_max'] = 'N/A'
            info['cpu_freq_current'] = 'N/A'
        
        # Add memory information
        try:
            mem = psutil.virtual_memory()
            info['memory_total_gb'] = round(mem.total / (1024**3), 2)
            info['memory_available_gb'] = round(mem.available / (1024**3), 2)
        except Exception:
            info['memory_total_gb'] = 'N/A'
            info['memory_available_gb'] = 'N/A'
        
        # Add Python information
        info['python_version'] = platform.python_version()
        info['python_implementation'] = platform.python_implementation()
        
        # Enhanced CPU information using cpuinfo
        try:
            import cpuinfo
            cpu_info = cpuinfo.get_cpu_info()
            info['cpu_brand'] = cpu_info.get('brand_raw', 'Unknown')
            info['cpu_arch'] = cpu_info.get('arch', 'Unknown')
        except ImportError:
            info['cpu_brand'] = 'Unknown (cpuinfo not available)'
            info['cpu_arch'] = 'Unknown'
        except Exception:
            # Fallback to basic platform information
            info['cpu_brand'] = info['processor']
            info['cpu_arch'] = info['machine']
        
        return info
        
    except Exception as e:
        # Minimal fallback without logging
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'hostname': socket.gethostname(),
            'platform': platform.platform(),
            'system': platform.system(),  # Fallback to basic detection
            'error': f'Host info collection failed: {e}'
        }


def _get_enhanced_system_name(detector) -> str:
    """
    Get enhanced system name with WSL detection.
    
    Args:
        detector: PlatformDetector instance
        
    Returns:
        str: Enhanced system name (Windows, Linux, WSL1, WSL2, WSL3, Darwin)
    """
    if detector.IS_WSL:
        # Return specific WSL version for future compatibility
        if detector.WSL_VERSION:
            # Handle known versions and future versions
            if 'WSL1' in detector.WSL_VERSION:
                return 'WSL1'
            elif 'WSL2' in detector.WSL_VERSION:
                return 'WSL2'
            elif 'WSL3' in detector.WSL_VERSION:  # Future-proofing
                return 'WSL3'
            else:
                return 'WSL2'  # Default to WSL2 for unknown WSL versions
        else:
            return 'WSL2'  # Default assumption for detected WSL
    
    # For non-WSL systems, use standard platform.system() names
    return platform.system()


class SystemInfo:
    """
    Comprehensive system information collection with platform integration.
    
    This class provides enhanced system information including platform flags,
    library availability, and platform-specific recommendations.
    """
    
    @staticmethod
    def get_all() -> Dict[str, Any]:
        """
        Get comprehensive system information including platform flags and library availability.
        
        Returns:
            Dict[str, Any]: Complete system information with enhanced platform detection
        """
        try:
            # Try relative import first (when imported as module)
            from .platform_utils import PlatformDetector
        except ImportError:
            # Fallback to absolute import (when run as script)
            try:
                from platform_utils import PlatformDetector
            except ImportError:
                # If still fails, provide basic info without platform detection
                return get_host_info()
        
        # Get basic host information with enhanced platform detection
        info = get_host_info()
        
        # Create detector instance for additional information
        detector = PlatformDetector()
        
        # Add platform flags for programmatic use
        platform_flags = detector.get_platform_flags()
        info.update(platform_flags)
        
        # Add library availability information
        lib_availability = detector.get_library_availability()
        info.update(lib_availability)
        
        # Add platform-specific capabilities
        capabilities = detector.get_system_capabilities()
        info.update({
            'platform_capabilities': capabilities,
            'platform_recommendations': detector.get_platform_specific_recommendations()
        })
        
        return info
    
    @staticmethod
    def get_platform_summary() -> str:
        """
        Get a concise platform summary string for display.
        
        Returns:
            str: Human-readable platform summary
        """
        from .platform_utils import PlatformDetector
        detector = PlatformDetector()
        
        if detector.IS_WINDOWS:
            return "Windows (Native)"
        elif detector.IS_WSL:
            wsl_version = detector.WSL_VERSION or "WSL"
            return f"Linux ({wsl_version})"
        elif detector.IS_LINUX:
            distro = getattr(detector, 'LINUX_DISTRO', 'Linux')
            return f"Linux ({distro})"
        elif detector.IS_MACOS:
            return "macOS"
        else:
            return "Unknown Platform"
    
    @staticmethod
    def get_platform_flags() -> Dict[str, bool]:
        """
        Get platform detection flags for programmatic use.
        
        Returns:
            Dict[str, bool]: Platform detection flags
        """
        from .platform_utils import PlatformDetector
        detector = PlatformDetector()
        return detector.get_platform_flags()
    
    @staticmethod
    def get_benchmark_environment_info() -> Dict[str, Any]:
        """
        Get environment information specifically relevant for benchmarking.
        
        Returns:
            Dict[str, Any]: Benchmark-relevant environment information
        """
        from .platform_utils import PlatformDetector
        detector = PlatformDetector()
        
        host_info = get_host_info()
        
        return {
            'system_type': host_info['system'],
            'available_libraries': detector.get_available_benchmark_libraries(),
            'platform_recommendations': detector.get_platform_specific_recommendations(),
            'memory_total_gb': host_info.get('memory_total_gb', 'Unknown'),
            'cpu_count_logical': host_info.get('cpu_count_logical', 'Unknown'),
            'cpu_brand': host_info.get('cpu_brand', 'Unknown'),
            'platform_summary': SystemInfo.get_platform_summary()
        }


def get_csv_compatible_host_info() -> Dict[str, Any]:
    """
    Get host information formatted for CSV export compatibility.
    
    This function ensures all values are CSV-compatible and maintains
    the exact column order expected by benchmark scripts.
    
    Returns:
        Dict[str, Any]: Host information ready for CSV export
    """
    info = get_host_info()
    
    # Ensure all values are CSV-compatible (convert None to empty string, etc.)
    for key, value in info.items():
        if value is None:
            info[key] = ''
        elif value == 'N/A':
            info[key] = ''
        elif isinstance(value, (int, float)) and value != value:  # NaN check
            info[key] = ''
    
    return info


if __name__ == "__main__":
    """Demo and testing functionality."""
    print("=== Basic Host Information ===")
    host_info = get_host_info()
    for key, value in host_info.items():
        print(f"{key}: {value}")
    
    print("\n=== Complete System Information ===")
    system_info = SystemInfo.get_all()
    for key, value in system_info.items():
        if not key.startswith('_'):  # Skip internal keys
            if isinstance(value, dict):
                print(f"{key}: {len(value)} items")
            else:
                print(f"{key}: {value}")
    
    print(f"\n=== Platform Summary ===")
    print(f"Platform: {SystemInfo.get_platform_summary()}")
    
    print(f"\n=== Benchmark Environment ===")
    bench_info = SystemInfo.get_benchmark_environment_info()
    for key, value in bench_info.items():
        print(f"{key}: {value}")