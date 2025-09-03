"""
Universal Data I/O Module - Reusable across projects

This module provides universal file reading capabilities across different formats
and data processing libraries. Designed for reusability and extensibility.

Features:
- Auto-detection of file formats (CSV, Parquet, JSON, NDJSON, compressed)
- Support for multiple data libraries (pandas, polars, modin, duckdb)
- Compression handling (gzip, zip, zstandard)
- Memory-efficient reading with optional column selection
- Robust error handling and fallbacks
"""

from pathlib import Path
from typing import Union, Any, Optional, List, Dict
import warnings


class UniversalDataReader:
    """
    Universal data reader that supports multiple formats and libraries.
    
    Design Philosophy:
    - Format-agnostic: automatically detect and handle different file formats
    - Library-agnostic: work with pandas, polars, modin, duckdb
    - Compression-aware: handle .gz, .zip, .zst transparently
    - Performance-oriented: optimize for large datasets
    - Error-resilient: graceful fallbacks and clear error messages
    """
    
    SUPPORTED_FORMATS = {
        '.csv': 'csv',
        '.parquet': 'parquet',
        '.json': 'json',
        '.jsonl': 'ndjson',
        '.ndjson': 'ndjson',
        '.txt': 'csv',  # Often tab-separated or CSV
    }
    
    COMPRESSION_FORMATS = {'.gz', '.zip', '.zst', '.bz2'}
    
    def __init__(self, default_library: str = 'pandas'):
        """
        Initialize the universal data reader.
        
        Args:
            default_library: Default library to use ('pandas', 'polars', 'modin')
        """
        self.default_library = default_library
        self._check_library_availability()
    
    def _check_library_availability(self):
        """Check which data processing libraries are available."""
        self.available_libraries = {}
        
        try:
            import pandas as pd
            self.available_libraries['pandas'] = pd
        except ImportError:
            pass
            
        try:
            import polars as pl
            self.available_libraries['polars'] = pl
        except ImportError:
            pass
            
        try:
            import modin.pandas as mpd
            self.available_libraries['modin'] = mpd
        except ImportError:
            pass
            
        try:
            import duckdb
            self.available_libraries['duckdb'] = duckdb
        except ImportError:
            pass
    
    def detect_file_format(self, file_path: Path) -> str:
        """
        Detect file format based on extension and content.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Detected format string ('csv', 'parquet', 'json', 'ndjson')
        """
        if not isinstance(file_path, Path):
            file_path = Path(file_path)
        
        suffixes = file_path.suffixes
        
        # Handle compressed files - look at the format before compression
        if any(suffix in self.COMPRESSION_FORMATS for suffix in suffixes):
            # Get the format extension before compression
            clean_suffixes = [s for s in suffixes if s not in self.COMPRESSION_FORMATS]
            if clean_suffixes:
                format_ext = clean_suffixes[-1]
            else:
                format_ext = '.csv'  # Default assumption
        else:
            format_ext = file_path.suffix
        
        return self.SUPPORTED_FORMATS.get(format_ext.lower(), 'csv')
    
    def is_compressed(self, file_path: Path) -> bool:
        """Check if file is compressed."""
        return any(suffix in self.COMPRESSION_FORMATS for suffix in file_path.suffixes)
    
    def get_compression_type(self, file_path: Path) -> Optional[str]:
        """Get compression type if file is compressed."""
        for suffix in file_path.suffixes:
            if suffix == '.gz':
                return 'gzip'
            elif suffix == '.zip':
                return 'zip'
            elif suffix == '.zst':
                return 'zstandard'
            elif suffix == '.bz2':
                return 'bz2'
        return None
    
    def read_file(self, 
                  file_path: Union[str, Path], 
                  library: Optional[str] = None,
                  usecols: Optional[List[str]] = None,
                  nrows: Optional[int] = None,
                  **kwargs) -> Any:
        """
        Universal file reader supporting multiple formats and libraries.
        
        Args:
            file_path: Path to the file to read
            library: Library to use ('pandas', 'polars', 'modin', 'duckdb')
            usecols: Columns to read (for CSV/Parquet)
            nrows: Number of rows to read (for testing)
            **kwargs: Additional arguments passed to the reading function
            
        Returns:
            DataFrame object from the specified library
            
        Raises:
            ValueError: If unsupported library or format
            FileNotFoundError: If file doesn't exist
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Use default library if not specified
        if library is None:
            library = self.default_library
        
        # Check if library is available
        if library not in self.available_libraries:
            available = list(self.available_libraries.keys())
            raise ValueError(f"Library '{library}' not available. Available: {available}")
        
        file_format = self.detect_file_format(file_path)
        
        # Handle reading based on library and format
        try:
            return self._read_with_library(file_path, library, file_format, usecols, nrows, **kwargs)
        except Exception as e:
            # Fallback to pandas if available
            if library != 'pandas' and 'pandas' in self.available_libraries:
                warnings.warn(f"Failed to read with {library}, falling back to pandas: {e}")
                return self._read_with_library(file_path, 'pandas', file_format, usecols, nrows, **kwargs)
            else:
                raise
    
    def _read_with_library(self, file_path: Path, library: str, file_format: str, 
                          usecols: Optional[List[str]], nrows: Optional[int], **kwargs):
        """Read file with specific library and format."""
        lib = self.available_libraries[library]
        
        # Prepare common arguments
        read_kwargs = kwargs.copy()
        if usecols is not None:
            read_kwargs['usecols'] = usecols
        if nrows is not None:
            read_kwargs['nrows'] = nrows
        
        # Handle compression
        if self.is_compressed(file_path):
            compression = self.get_compression_type(file_path)
            if library in ['pandas', 'modin'] and file_format == 'csv':
                read_kwargs['compression'] = compression
        
        # Read based on library and format
        if library == 'pandas':
            return self._read_pandas(lib, file_path, file_format, read_kwargs)
        elif library == 'polars':
            return self._read_polars(lib, file_path, file_format, read_kwargs)
        elif library == 'modin':
            return self._read_modin(lib, file_path, file_format, read_kwargs)
        elif library == 'duckdb':
            return self._read_duckdb(lib, file_path, file_format, read_kwargs)
        else:
            raise ValueError(f"Unsupported library: {library}")
    
    def _read_pandas(self, pd, file_path: Path, file_format: str, kwargs: Dict):
        """Read file with pandas."""
        if file_format == 'csv':
            return pd.read_csv(file_path, **kwargs)
        elif file_format == 'parquet':
            # Remove CSV-specific kwargs for parquet
            parquet_kwargs = {k: v for k, v in kwargs.items() 
                            if k not in ['compression', 'delimiter', 'sep']}
            return pd.read_parquet(file_path, **parquet_kwargs)
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in kwargs.items() 
                          if k not in ['compression', 'delimiter', 'sep', 'usecols']}
            return pd.read_json(file_path, **json_kwargs)
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in kwargs.items() 
                          if k not in ['compression', 'delimiter', 'sep', 'usecols']}
            return pd.read_json(file_path, lines=True, **json_kwargs)
    
    def _read_polars(self, pl, file_path: Path, file_format: str, kwargs: Dict):
        """Read file with polars."""
        # Polars has different parameter names
        polars_kwargs = kwargs.copy()
        
        if file_format == 'csv':
            return pl.read_csv(file_path, **polars_kwargs)
        elif file_format == 'parquet':
            parquet_kwargs = {k: v for k, v in polars_kwargs.items() 
                            if k not in ['compression', 'delimiter', 'sep']}
            return pl.read_parquet(file_path, **parquet_kwargs)
        elif file_format == 'json':
            return pl.read_json(file_path)
        elif file_format == 'ndjson':
            return pl.read_ndjson(file_path)
    
    def _read_modin(self, mpd, file_path: Path, file_format: str, kwargs: Dict):
        """Read file with modin."""
        if file_format == 'csv':
            return mpd.read_csv(file_path, **kwargs)
        elif file_format == 'parquet':
            parquet_kwargs = {k: v for k, v in kwargs.items() 
                            if k not in ['compression', 'delimiter', 'sep']}
            return mpd.read_parquet(file_path, **parquet_kwargs)
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in kwargs.items() 
                          if k not in ['compression', 'delimiter', 'sep', 'usecols']}
            return mpd.read_json(file_path, **json_kwargs)
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in kwargs.items() 
                          if k not in ['compression', 'delimiter', 'sep', 'usecols']}
            return mpd.read_json(file_path, lines=True, **json_kwargs)
    
    def _read_duckdb(self, duckdb, file_path: Path, file_format: str, kwargs: Dict):
        """Read file with duckdb and return as pandas DataFrame."""
        conn = duckdb.connect()
        
        try:
            if file_format == 'csv':
                query = f"SELECT * FROM read_csv_auto('{file_path}')"
            elif file_format == 'parquet':
                query = f"SELECT * FROM read_parquet('{file_path}')"
            elif file_format in ['json', 'ndjson']:
                query = f"SELECT * FROM read_json_auto('{file_path}')"
            else:
                raise ValueError(f"DuckDB doesn't support format: {file_format}")
            
            # Add LIMIT if nrows specified
            if 'nrows' in kwargs and kwargs['nrows'] is not None:
                query += f" LIMIT {kwargs['nrows']}"
            
            # Add column selection if usecols specified
            if 'usecols' in kwargs and kwargs['usecols'] is not None:
                cols = ', '.join(kwargs['usecols'])
                query = query.replace("SELECT *", f"SELECT {cols}")
            
            result = conn.execute(query).fetchdf()
            return result
        finally:
            conn.close()


class DatasetFinder:
    """
    Smart dataset finder that locates the best available dataset file.
    
    Design for reusability:
    - Configurable search patterns
    - Priority-based selection
    - Multiple search directories
    - Extensible file preferences
    """
    
    def __init__(self, search_dirs: Optional[List[Path]] = None, 
                 file_patterns: Optional[List[str]] = None):
        """
        Initialize dataset finder.
        
        Args:
            search_dirs: Directories to search for datasets
            file_patterns: File patterns in order of preference
        """
        self.search_dirs = search_dirs or []
        self.file_patterns = file_patterns or [
            # Preference order: Size then format preference
            "*_7M.parquet", "*_7M.csv",
            "*_10M.parquet", "*_10M.csv", 
            "*_5M.parquet", "*_5M.csv",
            "*_1M.parquet", "*_1M.csv",
            "*.parquet", "*.csv",
            "*.ndjson", "*.jsonl", "*.json",
            "*.csv.gz", "*.parquet.gz"
        ]
    
    def find_dataset(self, project_root: Optional[Path] = None, 
                    data_subdir: str = "data/raw") -> Optional[Path]:
        """
        Find the best available dataset file.
        
        Args:
            project_root: Root directory of the project
            data_subdir: Subdirectory containing data files
            
        Returns:
            Path to the best dataset file, or None if not found
        """
        search_paths = []
        
        # Add project-specific search paths
        if project_root:
            search_paths.append(project_root / data_subdir)
            search_paths.append(project_root / "data")
            search_paths.append(project_root)
        
        # Add configured search directories
        search_paths.extend(self.search_dirs)
        
        # Search for files in order of preference
        for pattern in self.file_patterns:
            for search_dir in search_paths:
                if not search_dir.exists():
                    continue
                    
                matches = list(search_dir.glob(pattern))
                if matches:
                    # Return the first match (most preferred)
                    best_file = matches[0]
                    print(f"Found dataset: {best_file}")
                    return best_file
        
        return None


# Convenience functions for easy use across projects
_default_reader = UniversalDataReader()
_default_finder = DatasetFinder()

def read_data(file_path: Union[str, Path], 
              library: str = 'pandas', 
              **kwargs) -> Any:
    """
    Convenience function to read data files with automatic format detection.
    
    Args:
        file_path: Path to the file
        library: Data processing library to use
        **kwargs: Additional arguments for reading
        
    Returns:
        DataFrame from the specified library
    """
    return _default_reader.read_file(file_path, library=library, **kwargs)

def find_dataset(project_root: Optional[Path] = None) -> Optional[Path]:
    """
    Convenience function to find the best dataset in a project.
    
    Args:
        project_root: Root directory of the project
        
    Returns:
        Path to the best dataset file
    """
    return _default_finder.find_dataset(project_root)

def get_dataset_size(file_path: Union[str, Path]) -> int:
    """
    Get the number of records in a dataset file.
    
    Args:
        file_path: Path to the dataset file
        
    Returns:
        Number of records in the dataset
    """
    try:
        df = read_data(file_path, library='pandas', nrows=None)
        return len(df)
    except Exception as e:
        print(f"Warning: Could not determine dataset size: {e}")
        return 0
