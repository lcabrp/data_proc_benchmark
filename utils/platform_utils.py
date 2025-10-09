"""
Platform detection and library availability utilities.

Provides centralized platform detection, library availability checking,
and environment-specific configuration recommendations for benchmarking.
"""

import platform
import sys
import os
from typing import Dict, Any, Optional, Tuple


class PlatformDetector:
    """Centralized platform detection and configuration for cross-platform benchmarking."""
    
    def __init__(self):
        self.system = platform.system()
        self.release = platform.release()
        
        # Core platform flags
        self.IS_WINDOWS = self.system == "Windows"
        self.IS_LINUX = self.system == "Linux"
        self.IS_MACOS = self.system == "Darwin"
        
        # Enhanced WSL detection with fallback to original method
        self.IS_WSL, self.WSL_VERSION = self._detect_wsl()
        self.IS_NATIVE_LINUX = self.IS_LINUX and not self.IS_WSL
        
        # Linux distribution (if applicable)
        if self.IS_LINUX:
            self.LINUX_DISTRO = self._check_linux_distribution()
        else:
            self.LINUX_DISTRO = None
        
        # Library availability detection
        self._check_library_availability()
    
    def _detect_wsl(self) -> Tuple[bool, Optional[str]]:
        """
        Enhanced WSL detection with multiple methods and fallback.
        Returns: (is_wsl, wsl_version)
        """
        if not self.IS_LINUX:
            return False, None
        
        # Method 1: Check /proc/version (most reliable)
        try:
            with open('/proc/version', 'r') as f:
                version_info = f.read().lower()
                if 'microsoft' in version_info:
                    if 'wsl2' in version_info:
                        return True, 'WSL2'
                    elif 'wsl' in version_info:
                        return True, 'WSL1'
                    else:
                        return True, 'WSL_UNKNOWN'
        except (FileNotFoundError, PermissionError):
            pass
        
        # Method 2: Check environment variables
        wsl_env = os.environ.get('WSL_DISTRO_NAME')
        if wsl_env:
            # WSL2 has WSL_INTEROP, WSL1 typically doesn't
            wsl_interop = os.environ.get('WSL_INTEROP')
            return True, 'WSL2' if wsl_interop else 'WSL1'
        
        # Method 3: Fallback to original method (maintains backward compatibility)
        if 'wsl' in self.release.lower():
            return True, 'WSL_RELEASE'
        
        return False, None
    
    def _check_linux_distribution(self) -> str:
        """Detect specific Linux distribution."""
        try:
            import distro
            return distro.name()
        except ImportError:
            try:
                with open('/etc/os-release', 'r') as f:
                    for line in f:
                        if line.startswith('NAME='):
                            return line.split('=')[1].strip().strip('"')
            except FileNotFoundError:
                pass
        return "Unknown Linux"
    
    def _check_library_availability(self):
        """Check availability of optional libraries and data processing frameworks."""
        
        # FireDucks (Linux/macOS only)
        self.FIREDUCKS_AVAILABLE = False
        if self.IS_LINUX or self.IS_MACOS:
            try:
                import fireducks.pandas as fpd
                self.FIREDUCKS_AVAILABLE = True
            except ImportError:
                pass
        
        # Core data processing libraries (should always be available)
        self.PANDAS_AVAILABLE = True  # Required dependency
        
        try:
            import polars
            self.POLARS_AVAILABLE = True
        except ImportError:
            self.POLARS_AVAILABLE = False
        
        try:
            import duckdb
            self.DUCKDB_AVAILABLE = True
        except ImportError:
            self.DUCKDB_AVAILABLE = False
        
        # Optional system info libraries
        try:
            import cpuinfo
            self.CPUINFO_AVAILABLE = True
        except ImportError:
            self.CPUINFO_AVAILABLE = False
        
        try:
            import wmi
            self.WMI_AVAILABLE = True
        except ImportError:
            self.WMI_AVAILABLE = False
        
        try:
            import GPUtil
            self.GPUTIL_AVAILABLE = True
        except ImportError:
            self.GPUTIL_AVAILABLE = False
    
    def get_platform_flags(self) -> Dict[str, bool]:
        """Get platform detection flags (maintains original API)."""
        return {
            'IS_WINDOWS': self.IS_WINDOWS,
            'IS_LINUX': self.IS_LINUX,
            'IS_MACOS': self.IS_MACOS,
            'IS_WSL': self.IS_WSL
        }
    
    def get_enhanced_platform_flags(self) -> Dict[str, bool]:
        """Get enhanced platform detection flags with additional detail."""
        return {
            'IS_WINDOWS': self.IS_WINDOWS,
            'IS_LINUX': self.IS_LINUX,
            'IS_MACOS': self.IS_MACOS,
            'IS_WSL': self.IS_WSL,
            'IS_NATIVE_LINUX': self.IS_NATIVE_LINUX
        }
    
    def get_library_availability(self) -> Dict[str, bool]:
        """Get availability status of all libraries (maintains original API)."""
        return {
            'pandas': self.PANDAS_AVAILABLE,
            'polars': self.POLARS_AVAILABLE,
            'duckdb': self.DUCKDB_AVAILABLE,
            'fireducks': self.FIREDUCKS_AVAILABLE,
            'cpuinfo': self.CPUINFO_AVAILABLE,
            'wmi': self.WMI_AVAILABLE,
            'gputil': self.GPUTIL_AVAILABLE
        }
    
    def get_available_benchmark_libraries(self) -> list:
        """Get list of available data processing libraries for benchmarking."""
        libraries = []
        if self.PANDAS_AVAILABLE:
            libraries.append('pandas')
        if self.POLARS_AVAILABLE:
            libraries.append('polars')
        if self.DUCKDB_AVAILABLE:
            libraries.append('duckdb')
        if self.FIREDUCKS_AVAILABLE:
            libraries.append('fireducks')
        return libraries
    
    def get_system_capabilities(self) -> Dict[str, Any]:
        """Get system capabilities and recommendations (maintains original API)."""
        capabilities = {
            'platform': self.system,
            'is_windows': self.IS_WINDOWS,
            'is_linux': self.IS_LINUX,
            'is_macos': self.IS_MACOS,
            'is_wsl': self.IS_WSL,
            'python_version': sys.version,
            'available_libraries': self.get_available_benchmark_libraries(),
            'optional_libraries': {
                'cpuinfo': self.CPUINFO_AVAILABLE,
                'wmi': self.WMI_AVAILABLE,
                'gputil': self.GPUTIL_AVAILABLE
            }
        }
        
        # Add memory recommendations
        try:
            import psutil
            memory_gb = psutil.virtual_memory().total / (1024**3)
            capabilities['total_memory_gb'] = round(memory_gb, 1)
            capabilities['memory_recommendation'] = self._get_memory_recommendation(memory_gb)
        except ImportError:
            capabilities['total_memory_gb'] = 'unknown'
            capabilities['memory_recommendation'] = 'Install psutil for memory analysis'
        
        return capabilities
    
    def get_enhanced_system_capabilities(self) -> Dict[str, Any]:
        """Get enhanced system capabilities with additional WSL/Linux details."""
        capabilities = self.get_system_capabilities()
        
        # Add enhanced platform details
        capabilities['is_native_linux'] = self.IS_NATIVE_LINUX
        
        if self.IS_WSL:
            capabilities['wsl_version'] = self.WSL_VERSION
        
        if self.IS_LINUX:
            capabilities['linux_distro'] = self.LINUX_DISTRO
        
        return capabilities
    
    def _get_memory_recommendation(self, memory_gb: float) -> str:
        """Get memory usage recommendations based on available RAM."""
        if memory_gb < 4:
            return "Low memory - consider using smaller datasets or cloud processing"
        elif memory_gb < 8:
            return "Moderate memory - suitable for datasets up to 1M records"
        elif memory_gb < 16:
            return "Good memory - suitable for datasets up to 10M records"
        elif memory_gb < 32:
            return "High memory - suitable for datasets up to 100M records"
        else:
            return "Very high memory - suitable for large-scale processing"
    
    def get_platform_specific_recommendations(self) -> Dict[str, str]:
        """Get platform-specific recommendations for optimal performance."""
        recommendations = {}
        
        if self.IS_WINDOWS:
            recommendations.update({
                'file_format': 'Use Parquet format for better Windows I/O performance',
                'memory': 'Windows may require more conservative memory settings',
                'fireducks': 'FireDucks not available on Windows - use pandas/polars/duckdb'
            })
        
        if self.IS_WSL:
            wsl_version = self.WSL_VERSION or 'WSL'
            recommendations.update({
                'performance': f'{wsl_version} may have I/O overhead - consider native Linux for best performance',
                'file_access': 'Store datasets on WSL filesystem for better performance',
                'memory': f'{wsl_version} shares memory with Windows - monitor usage carefully',
                'fireducks': f'FireDucks available on {wsl_version} but may have reduced performance'
            })
        
        if self.IS_NATIVE_LINUX:
            distro = self.LINUX_DISTRO or 'Linux'
            recommendations.update({
                'performance': f'Native {distro} - optimal performance expected',
                'fireducks': 'FireDucks available for advanced pandas acceleration',
                'memory': 'Linux typically handles memory most efficiently'
            })
        
        if self.IS_MACOS:
            recommendations.update({
                'fireducks': 'FireDucks available for advanced pandas acceleration',
                'memory': 'macOS handles memory efficiently but may have M1/Intel differences'
            })
        
        return recommendations


# Backward compatibility - expose functions directly (UNCHANGED API)
def get_platform_flags() -> Dict[str, bool]:
    """Get platform detection flags (backward compatibility)."""
    detector = PlatformDetector()
    return detector.get_platform_flags()


def check_library_availability() -> Dict[str, bool]:
    """Check availability of libraries (backward compatibility)."""
    detector = PlatformDetector()
    return detector.get_library_availability()


def get_system_info() -> Dict[str, Any]:
    """Get comprehensive system information (backward compatibility)."""
    detector = PlatformDetector()
    return detector.get_system_capabilities()


# New enhanced functions (optional for benchmark scripts that want more detail)
def get_enhanced_platform_info() -> Dict[str, Any]:
    """Get enhanced platform information with WSL/Linux details."""
    detector = PlatformDetector()
    return {
        'platform_flags': detector.get_enhanced_platform_flags(),
        'system_capabilities': detector.get_enhanced_system_capabilities(),
        'recommendations': detector.get_platform_specific_recommendations()
    }


# Module-level constants for direct access (MAINTAINS ORIGINAL API)
_detector = PlatformDetector()

IS_WINDOWS = _detector.IS_WINDOWS
IS_LINUX = _detector.IS_LINUX
IS_MACOS = _detector.IS_MACOS
IS_WSL = _detector.IS_WSL

# New constants (optional, won't break existing code)
IS_NATIVE_LINUX = _detector.IS_NATIVE_LINUX
WSL_VERSION = _detector.WSL_VERSION
LINUX_DISTRO = _detector.LINUX_DISTRO

PANDAS_AVAILABLE = _detector.PANDAS_AVAILABLE
POLARS_AVAILABLE = _detector.POLARS_AVAILABLE
DUCKDB_AVAILABLE = _detector.DUCKDB_AVAILABLE
FIREDUCKS_AVAILABLE = _detector.FIREDUCKS_AVAILABLE

CPUINFO_AVAILABLE = _detector.CPUINFO_AVAILABLE
WMI_AVAILABLE = _detector.WMI_AVAILABLE
GPUTIL_AVAILABLE = _detector.GPUTIL_AVAILABLE


# Clean up temporary detector
del _detector