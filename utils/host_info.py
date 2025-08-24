"""
Host Information Collection Utilities

This module provides utilities for collecting system information across different platforms.
Optimized for reliable operation without blocking WMI calls on Windows.
"""

import datetime
import platform
import socket
import psutil
from typing import Dict, Any, Optional

def get_host_info() -> dict:
    """
    Collect basic host system information without potentially blocking operations.
    Returns:
        dict: Host system information.
    """
    try:
        info = {
            'timestamp': datetime.datetime.now().isoformat(),
            'hostname': socket.gethostname(),
            'platform': platform.platform(),
            'system': platform.system(),
            'release': platform.release(),
            'version': platform.version(),
            'machine': platform.machine(),
            'processor': platform.processor(),
            'cpu_count_logical': psutil.cpu_count(logical=True),
            'cpu_count_physical': psutil.cpu_count(logical=False),
        }
        
        # Add CPU frequency if available (in correct order)
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
        
        # Add memory info (in correct order)
        try:
            mem = psutil.virtual_memory()
            info['memory_total_gb'] = round(mem.total / (1024**3), 2)
            info['memory_available_gb'] = round(mem.available / (1024**3), 2)
        except Exception:
            info['memory_total_gb'] = 'N/A'
            info['memory_available_gb'] = 'N/A'
        
        # Add Python info (in correct order)
        info['python_version'] = platform.python_version()
        info['python_implementation'] = platform.python_implementation()
        
        # Try cpuinfo for enhanced CPU info
        try:
            import cpuinfo
            cpu_info = cpuinfo.get_cpu_info()
            info['cpu_brand'] = cpu_info.get('brand_raw', 'Unknown')
            info['cpu_arch'] = cpu_info.get('arch', 'Unknown')
        except ImportError:
            info['cpu_brand'] = 'Unknown (cpuinfo not available)'
            info['cpu_arch'] = 'Unknown'
        except Exception:
            info['cpu_brand'] = info['processor']
            info['cpu_arch'] = info['machine']
        
        return info
    except Exception as e:
        # Top-level fallback: return minimal info
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'hostname': socket.gethostname(),
            'error': f'Host info collection failed: {e}'
        }


class SystemInfo:
    """Comprehensive system information collection with platform integration."""
    
    @staticmethod
    def get_all() -> Dict[str, Any]:
        """
        Get comprehensive system information including platform flags and library availability.
        
        Returns:
            Dict[str, Any]: Complete system information
        """
        from .platform_utils import PlatformDetector
        
        info = get_host_info()
        
        # Add platform flags
        platform_flags = PlatformDetector.get_platform_flags()
        info.update(platform_flags)
        
        # Add library availability
        lib_availability = PlatformDetector.check_library_availability()
        info.update(lib_availability)
        
        # Add recommended Modin engine
        info['recommended_modin_engine'] = PlatformDetector.get_recommended_modin_engine()
        
        return info
    
    @staticmethod
    def get_platform_summary() -> str:
        """Get a concise platform summary string."""
        flags = SystemInfo.get_platform_flags()
        if flags['IS_WINDOWS']:
            return f"Windows ({'WSL' if flags['IS_WSL'] else 'Native'})"
        elif flags['IS_LINUX']:
            return "Linux"
        elif flags['IS_MACOS']:
            return "macOS"
        else:
            return "Unknown"
    
    @staticmethod
    def get_platform_flags() -> Dict[str, bool]:
        """Get platform detection flags."""
        from .platform_utils import PlatformDetector
        return PlatformDetector.get_platform_flags()


if __name__ == "__main__":
    # Demo usage
    print("=== Host Information ===")
    host_info = get_host_info()
    for key, value in host_info.items():
        print(f"{key}: {value}")
    
    print("\n=== Complete System Information ===")
    system_info = SystemInfo.get_all()
    for key, value in system_info.items():
        if not key.startswith('_'):  # Skip internal keys
            print(f"{key}: {value}")
    
    print(f"\n=== Platform Summary ===")
    print(f"Platform: {SystemInfo.get_platform_summary()}")
