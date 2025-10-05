"""
Universal Data I/O Module - Reusable across projects

This module provides universal file reading capabilities across different formats
and data processing libraries. Designed for reusability and extensibility.

Features:
- Auto-detection of file formats (CSV, Parquet, JSON, NDJSON, compressed)
- Support for multiple data libraries (pandas, polars, duckdb, fireducks)
- Compression handling (gzip, zip, zstandard)
- Memory-efficient reading with optional column selection
- Robust error handling and fallbacks
"""

import pandas as pd
import polars as pl
import duckdb
from pathlib import Path
from typing import Union, Any, Optional, List, Dict
import warnings
import platform


class UniversalDataReader:
    """
    Universal data reader that supports multiple formats and libraries.
    
    Design Philosophy:
    - Format-agnostic: automatically detect and handle different file formats
    - Library-agnostic: work with pandas, polars, duckdb, fireducks
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
            default_library: Default library to use ('pandas', 'polars', 'duckdb', 'fireducks')
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
            import duckdb
            self.available_libraries['duckdb'] = duckdb
        except ImportError:
            pass
        
        # FireDucks: Linux/macOS only
        if platform.system() in ["Linux", "Darwin"]:
            try:
                import fireducks.pandas as fpd
                self.available_libraries['fireducks'] = fpd
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
            library: Library to use ('pandas', 'polars', 'duckdb', 'fireducks')
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

        read_kwargs = kwargs.copy()
        if usecols is not None:
            read_kwargs['usecols'] = usecols

        if self.is_compressed(file_path):
            compression = self.get_compression_type(file_path)
            if compression and library in ['pandas', 'fireducks'] and file_format == 'csv':
                read_kwargs['compression'] = compression

        if library == 'pandas':
            return self._read_pandas(lib, file_path, file_format, read_kwargs, nrows)
        elif library == 'polars':
            return self._read_polars(lib, file_path, file_format, read_kwargs, nrows)
        elif library == 'fireducks':
            return self._read_fireducks(lib, file_path, file_format, read_kwargs, nrows)
        elif library == 'duckdb':
            return self._read_duckdb(lib, file_path, file_format, read_kwargs, nrows)
        else:
            raise ValueError(f"Unsupported library: {library}")
    
    def _read_pandas(self, pd, file_path: Path, file_format: str, kwargs: Dict, nrows: Optional[int]):
        """Read file with pandas."""
        kwargs = kwargs.copy()
        usecols = kwargs.pop('usecols', None)

        if file_format == 'csv':
            return pd.read_csv(file_path, usecols=usecols, nrows=nrows, **kwargs)
        elif file_format == 'parquet':
            parquet_kwargs = {k: v for k, v in kwargs.items()
                              if k not in ['compression', 'delimiter', 'sep', 'nrows']}
            df = pd.read_parquet(file_path, columns=usecols, **parquet_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows']}
            df = pd.read_json(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows']}
            df = pd.read_json(file_path, lines=True, **json_kwargs)
            return df.head(nrows) if nrows else df
        else:
            raise ValueError(f"Unsupported format for pandas: {file_format}")

    def _read_polars(self, pl, file_path: Path, file_format: str, kwargs: Dict, nrows: Optional[int]):
        """Read file with polars."""
        polars_kwargs = kwargs.copy()
        columns = polars_kwargs.pop('usecols', None)

        if file_format == 'csv':
            if nrows is not None:
                polars_kwargs['n_rows'] = nrows
            if columns is not None:
                polars_kwargs['columns'] = columns
            return pl.read_csv(file_path, **polars_kwargs)
        elif file_format == 'parquet':
            if nrows is not None:
                polars_kwargs['n_rows'] = nrows
            if columns is not None:
                polars_kwargs['columns'] = columns
            parquet_kwargs = {k: v for k, v in polars_kwargs.items()
                              if k not in ['compression', 'delimiter', 'sep']}
            return pl.read_parquet(file_path, **parquet_kwargs)
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in polars_kwargs.items() if k not in ['columns', 'n_rows']}
            df = pl.read_json(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in polars_kwargs.items() if k not in ['columns', 'n_rows']}
            df = pl.read_ndjson(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        else:
            raise ValueError(f"Unsupported format for polars: {file_format}")

    def _read_duckdb(self, duckdb, file_path: Path, file_format: str, kwargs: Dict, nrows: Optional[int]):
        """Read file with duckdb and return as pandas DataFrame."""
        kwargs = kwargs.copy()
        usecols = kwargs.pop('usecols', None)

        conn = duckdb.connect()
        try:
            path_str = str(file_path)
            if file_format == 'csv':
                query = "SELECT * FROM read_csv_auto(?)"
            elif file_format == 'parquet':
                query = "SELECT * FROM read_parquet(?)"
            elif file_format in ['json', 'ndjson']:
                query = "SELECT * FROM read_json_auto(?)"
            else:
                raise ValueError(f"DuckDB doesn't support format: {file_format}")

            if usecols:
                cols = ', '.join(f'"{col.replace("\"", "\"\"")}"' for col in usecols)
                query = query.replace("SELECT *", f"SELECT {cols}")

            if nrows is not None:
                query += f" LIMIT {int(nrows)}"

            return conn.execute(query, [path_str]).fetchdf()
        finally:
            conn.close()

    def _read_fireducks(self, fpd, file_path: Path, file_format: str, kwargs: Dict, nrows: Optional[int]):
        """Read file with fireducks."""
        kwargs = kwargs.copy()
        usecols = kwargs.pop('usecols', None)

        if file_format == 'csv':
            return fpd.read_csv(file_path, usecols=usecols, nrows=nrows, **kwargs)
        elif file_format == 'parquet':
            parquet_kwargs = {k: v for k, v in kwargs.items()
                              if k not in ['compression', 'delimiter', 'sep', 'nrows']}
            df = fpd.read_parquet(file_path, columns=usecols, **parquet_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows']}
            df = fpd.read_json(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows']}
            df = fpd.read_json(file_path, lines=True, **json_kwargs)
            return df.head(nrows) if nrows else df
        else:
            raise ValueError(f"Unsupported format for fireducks: {file_format}")


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
def read_data(
    file_path: Path,
    library: str = "pandas",
    nrows: Optional[int] = None,
    **kwargs
) -> Optional[Union[pd.DataFrame, pl.DataFrame]]:
    """
    Read data from various file formats using specified library.
    """
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return None

    merged_kwargs = kwargs.copy()
    usecols = merged_kwargs.pop('usecols', None)

    try:
        return _default_reader.read_file(
            file_path=file_path,
            library=library,
            usecols=usecols,
            nrows=nrows,
            **merged_kwargs
        )
    except Exception as e:
        print(f"Error reading {file_path} with {library}: {e}")
        return None

def find_dataset() -> Optional[Path]:
    """
    Find the default dataset file in the data/raw directory.
    Returns the Path if found, else None.
    """
    import os
    from pathlib import Path
    data_dir = Path("data/raw")
    for ext in [".csv", ".parquet", ".json", ".jsonl", ".ndjson"]:
        for file in data_dir.glob(f"*{ext}"):
            return file.resolve()
    return None

def get_dataset_size(file_path: Path) -> int:
    """
    Return the number of rows in the dataset for supported formats.
    """
    if not file_path.exists():
        return 0
    ext = file_path.suffix.lower()
    if ext == ".csv":
        with open(file_path, "r") as f:
            return sum(1 for _ in f) - 1  # subtract header
    elif ext == ".parquet":
        try:
            import pandas as pd
            df = pd.read_parquet(file_path, columns=None)
            return len(df)
        except Exception:
            return 0
    elif ext in [".json", ".jsonl", ".ndjson"]:
        with open(file_path, "r") as f:
            return sum(1 for _ in f)
    else:
        return 0

_default_reader = UniversalDataReader()
