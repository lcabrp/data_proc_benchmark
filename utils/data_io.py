"""
Universal Data I/O Module - Reusable across projects

This module provides universal file reading capabilities across different formats
and data processing libraries, delegating under the hood to the CleanFlow library.
"""

import warnings
import platform
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl
import duckdb
import cleanflow


class UniversalDataReader:
    """
    Universal data reader that delegates to the optimized CleanFlow library.
    """
    
    SUPPORTED_FORMATS = {
        '.csv': 'csv',
        '.parquet': 'parquet',
        '.json': 'json',
        '.jsonl': 'ndjson',
        '.ndjson': 'ndjson',
        '.txt': 'csv',
    }
    
    COMPRESSION_FORMATS = {'.gz', '.zip', '.zst', '.bz2'}
    
    def __init__(self, default_library: str = 'pandas'):
        """
        Initialize the universal data reader.
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
        
        if platform.system() in ["Linux", "Darwin"]:
            try:
                import fireducks.pandas as fpd
                self.available_libraries['fireducks'] = fpd
            except ImportError:
                pass
    
    def detect_file_format(self, file_path: Path) -> str:
        """Detect file format based on extension and content."""
        if not isinstance(file_path, Path):
            file_path = Path(file_path)
        
        suffixes = file_path.suffixes
        
        if any(suffix in self.COMPRESSION_FORMATS for suffix in suffixes):
            clean_suffixes = [s for s in suffixes if s not in self.COMPRESSION_FORMATS]
            format_ext = clean_suffixes[-1] if clean_suffixes else '.csv'
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
        """Universal file reader supporting multiple formats and libraries, backed by CleanFlow."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if library is None:
            library = self.default_library
        
        if library not in self.available_libraries:
            available = list(self.available_libraries.keys())
            raise ValueError(f"Library '{library}' not available. Available: {available}")
        
        file_format = self.detect_file_format(file_path)
        
        try:
            return self._read_with_library(file_path, library, file_format, usecols, nrows, **kwargs)
        except Exception as e:
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
        """Read file with pandas, leveraging CleanFlow optimized loader."""
        kwargs = kwargs.copy()
        usecols = kwargs.pop('usecols', None)

        if file_format == 'csv':
            # Delegate to CleanFlow load_csv for PyArrow engine and date parsing support
            return cleanflow.io.load_csv(
                file_path,
                chunksize=None,
                type_map=kwargs.pop('type_map', None),
                use_dtype_hints=kwargs.pop('use_dtype_hints', True),
                **kwargs
            )
        elif file_format == 'parquet':
            parquet_kwargs = {k: v for k, v in kwargs.items()
                              if k not in ['compression', 'delimiter', 'sep', 'nrows', 'type_map', 'use_dtype_hints']}
            df = cleanflow.io.load_parquet(file_path)
            if usecols:
                df = df[usecols]
            return df.head(nrows) if nrows else df
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows', 'type_map', 'use_dtype_hints']}
            df = pd.read_json(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows', 'type_map', 'use_dtype_hints']}
            df = pd.read_json(file_path, lines=True, **json_kwargs)
            return df.head(nrows) if nrows else df
        else:
            raise ValueError(f"Unsupported format for pandas: {file_format}")

    def _read_polars(self, pl, file_path: Path, file_format: str, kwargs: Dict, nrows: Optional[int]):
        """Read file with polars, delegating to CleanFlow."""
        polars_kwargs = kwargs.copy()
        columns = polars_kwargs.pop('usecols', None)

        if file_format == 'csv':
            if nrows is not None:
                polars_kwargs['n_rows'] = nrows
            if columns is not None:
                polars_kwargs['columns'] = columns
            polars_kwargs.pop('type_map', None)
            polars_kwargs.pop('use_dtype_hints', None)
            return cleanflow.io.load_csv_polars(file_path)
        elif file_format == 'parquet':
            if nrows is not None:
                polars_kwargs['n_rows'] = nrows
            if columns is not None:
                polars_kwargs['columns'] = columns
            parquet_kwargs = {k: v for k, v in polars_kwargs.items()
                              if k not in ['compression', 'delimiter', 'sep', 'type_map', 'use_dtype_hints']}
            return cleanflow.io.load_parquet_polars(file_path)
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in polars_kwargs.items() if k not in ['columns', 'n_rows', 'type_map', 'use_dtype_hints']}
            df = pl.read_json(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in polars_kwargs.items() if k not in ['columns', 'n_rows', 'type_map', 'use_dtype_hints']}
            df = pl.read_ndjson(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        else:
            raise ValueError(f"Unsupported format for polars: {file_format}")

    def _read_duckdb(self, duckdb, file_path: Path, file_format: str, kwargs: Dict, nrows: Optional[int]):
        """Read file with duckdb using zero-copy PyArrow materialization."""
        from cleanflow.optimization.backends import duckdb_backend
        
        kwargs = kwargs.copy()
        usecols = kwargs.pop('usecols', None)
        
        # Optimize simple full-table loads with zero-copy Arrow output
        if not usecols and nrows is None:
            if file_format == 'csv':
                return duckdb_backend.load_csv(file_path, fetch_format='pandas')
            elif file_format == 'parquet':
                return duckdb_backend.load_parquet(file_path, fetch_format='pandas')
                
        # Fallback to compiled queries for projections and limits
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

            relation = conn.execute(query, [path_str])
            try:
                import pyarrow  # noqa: F401
                return relation.fetch_arrow_table().to_pandas()
            except ImportError:
                return relation.fetchdf()
        finally:
            conn.close()

    def _read_fireducks(self, fpd, file_path: Path, file_format: str, kwargs: Dict, nrows: Optional[int]):
        """Read file with fireducks, leveraging CleanFlow."""
        kwargs = kwargs.copy()
        usecols = kwargs.pop('usecols', None)

        if file_format == 'csv':
            # Delegate to CleanFlow load_csv
            df = cleanflow.io.load_csv(
                file_path,
                type_map=kwargs.pop('type_map', None),
                use_dtype_hints=kwargs.pop('use_dtype_hints', True),
                **kwargs
            )
            if not isinstance(df, fpd.DataFrame):
                df = fpd.DataFrame(df)
            return df
        elif file_format == 'parquet':
            parquet_kwargs = {k: v for k, v in kwargs.items()
                              if k not in ['compression', 'delimiter', 'sep', 'nrows', 'type_map', 'use_dtype_hints']}
            df = fpd.read_parquet(file_path, columns=usecols, **parquet_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'json':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows', 'type_map', 'use_dtype_hints']}
            df = fpd.read_json(file_path, **json_kwargs)
            return df.head(nrows) if nrows else df
        elif file_format == 'ndjson':
            json_kwargs = {k: v for k, v in kwargs.items()
                           if k not in ['compression', 'delimiter', 'sep', 'usecols', 'nrows', 'type_map', 'use_dtype_hints']}
            df = fpd.read_json(file_path, lines=True, **json_kwargs)
            return df.head(nrows) if nrows else df
        else:
            raise ValueError(f"Unsupported format for fireducks: {file_format}")


class DatasetFinder:
    """Smart dataset finder that locates the best available dataset file."""
    
    def __init__(self, search_dirs: Optional[List[Path]] = None, 
                 file_patterns: Optional[List[str]] = None):
        self.search_dirs = search_dirs or []
        self.file_patterns = file_patterns or [
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
    """Read data from various file formats using specified library."""
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
    """Find the default dataset file in the data/raw directory."""
    data_dir = Path("data/raw")
    for ext in [".csv", ".parquet", ".json", ".jsonl", ".ndjson"]:
        for file in data_dir.glob(f"*{ext}"):
            return file.resolve()
    return None


def get_dataset_size(file_path: Path) -> int:
    """Return the number of rows in the dataset for supported formats."""
    if not file_path.exists():
        return 0
    ext = file_path.suffix.lower()
    if ext == ".csv":
        try:
            # High-speed chunked line counter for massive files (10x faster than line-by-line read)
            count = 0
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024 * 10), b""):
                    count += chunk.count(b"\n")
            return max(count - 1, 0)  # subtract header row
        except Exception:
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    return sum(1 for _ in f) - 1
            except Exception:
                return 0
    elif ext == ".parquet":
        try:
            import pyarrow.parquet as pq
            return pq.read_metadata(file_path).num_rows
        except Exception:
            try:
                import pandas as pd
                df = pd.read_parquet(file_path)
                return len(df)
            except Exception:
                return 0
    elif ext in [".json", ".jsonl", ".ndjson"]:
        try:
            count = 0
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024 * 10), b""):
                    count += chunk.count(b"\n")
            return count
        except Exception:
            with open(file_path, "r") as f:
                return sum(1 for _ in f)
    else:
        return 0


_default_reader = UniversalDataReader()
