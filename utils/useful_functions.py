# useful_functions.py
from pathlib import Path
import pandas as pd
import glob
import sqlite3
import numpy as np
from typing import Union, Optional, Dict, List
import numbers

# Helper to convert GPU-accelerated (cuDF/cudf.pandas) DataFrame slices to plain pandas
# so that plotting libraries (seaborn/plotly) that expect CPU/NumPy arrays work reliably.
# It tries multiple safe conversion paths and falls back to a robust constructor.

def to_plain_pandas(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Return a real pandas.DataFrame with only the selected columns, even if the
    input is a cuDF DataFrame or a cudf.pandas-accelerated pandas object.

    Strategy (in order):
    1) Pandas interchange API (from_dataframe) when available
    2) cuDF -> pandas via .to_pandas()
    3) Generic pandas.DataFrame(dict(...)) fallback (robust, but slower)

    This is useful right before plotting or calling libraries that expect pure
    pandas/NumPy types (seaborn, plotly, scikit-learn without cuML backends).

    Args:
        df: Source DataFrame (pandas or cuDF-backed via cudf.pandas)
        cols: List of column names to extract

    Returns:
        pandas.DataFrame: A copy/slice containing the requested columns, with
        CPU-backed dtypes.
    """
    sub = df[cols]
    # Try pandas interchange API (works with cuDF >= 23.x and pandas >= 2.2)
    try:
        from pandas.api.interchange import from_dataframe
        pd_df = from_dataframe(sub)
        return pd_df
    except Exception:
        pass
    # Try cuDF's native conversion when available
    if hasattr(sub, "to_pandas"):
        try:
            return sub.to_pandas()
        except Exception:
            pass
    # Robust fallback: construct from Python lists (fine for moderate-sized plots)
    return pd.DataFrame({c: [v for v in sub[c]] for c in cols})

def get_files_dir(directory_path: str, file_mask: str = '*.csv') -> list:
    """
    Get all files matching the pattern in a directory.

    Args:
        directory_path: Path to the directory containing files
        file_mask: File pattern to match (default: '*.csv')

    Returns:
        list: List of file paths matching the pattern
    """
    # Ensure directory path ends with separator
    if not directory_path.endswith(('/', '\\')):
        directory_path += '/'

    # Check if directory exists
    if not Path(directory_path).exists():
        raise ValueError(f"Directory not found: {directory_path}")

    # Build the search pattern and get files
    pattern = directory_path + file_mask
    files = glob.glob(pattern)

    return files

def show_df_info(df: pd.DataFrame) -> None:
    """
    Display information about a DataFrame including the top few rows, column names, shape, and data types.
    """
    if type(df) != pd.DataFrame:
        raise TypeError("Expected a pandas DataFrame")

    print("\nFirst few rows of the DataFrame:")
    print(df.head(5))
    print("\nDataFrame columns:")
    print(list(df.columns))  # Print column names
    print(f"\nDataFrame shape: {df.shape}")  # Print shape
    print("\nDataFrame info:")
    print(df.info(memory_usage='deep'))  # Print info with deep memory usage

# https://stackoverflow.com/questions/57856010/automatically-optimizing-pandas-dtypes

# The following four functions are based on the above link
def optimize_types(dataframe):
    np_types = [np.int8 ,np.int16 ,np.int32, np.int64,
           np.uint8 ,np.uint16, np.uint32, np.uint64]
    np_types = [np_type.__name__ for np_type in np_types]
    type_df = pd.DataFrame(data=np_types, columns=['class_type'])
    type_df['min_value'] = type_df['class_type'].apply(lambda row: np.iinfo(row).min)
    type_df['max_value'] = type_df['class_type'].apply(lambda row: np.iinfo(row).max)
    type_df['range'] = type_df['max_value'] - type_df['min_value']
    type_df.sort_values(by='range', inplace=True)

    for col in dataframe.loc[:, dataframe.dtypes <= np.integer]:
        col_min = dataframe[col].min()
        col_max = dataframe[col].max()
        temp = type_df[(type_df['min_value'] <= col_min) & (type_df['max_value'] >= col_max)]
        optimized_class = temp.loc[temp['range'].idxmin(), 'class_type']
        print("Col name : {} Col min_value : {} Col max_value : {} Optimized Class : {}".format(col, col_min, col_max, optimized_class))
        dataframe[col] = dataframe[col].astype(optimized_class)
    return dataframe

def auto_opt_pd_dtypes(df_: pd.DataFrame, inplace=False) -> Optional[pd.DataFrame]:
    """ Automatically downcast Number dtypes for minimal possible,
        will not touch other (datetime, str, object, etc)

        :param df_: dataframe
        :param inplace: if False, will return a copy of input dataset

        :return: `None` if `inplace=True` or dataframe if `inplace=False`
    """
    df = df_ if inplace else df_.copy()

    for col in df.columns:
        # integers
        if issubclass(df[col].dtypes.type, numbers.Integral):
            # unsigned integers
            if df[col].min() >= 0:
                df[col] = pd.to_numeric(df[col], downcast='unsigned')
            # signed integers
            else:
                df[col] = pd.to_numeric(df[col], downcast='integer')
        # other real numbers
        elif issubclass(df[col].dtypes.type, numbers.Real):
            df[col] = pd.to_numeric(df[col], downcast='float')

    if not inplace:
        return df

def get_types(signed=True, unsigned=True, custom=[]):
    '''Returns a pandas dataframe containing the boundaries of each integer dtype'''
    # based on https://stackoverflow.com/a/57894540/9419492
    pd_types = custom
    if signed:
        pd_types += [pd.Int8Dtype() ,pd.Int16Dtype() ,pd.Int32Dtype(), pd.Int64Dtype()]
    if unsigned:
        pd_types += [pd.UInt8Dtype() ,pd.UInt16Dtype(), pd.UInt32Dtype(), pd.UInt64Dtype()]
    type_df = pd.DataFrame(data=pd_types, columns=['pd_type'])
    type_df['np_type'] = type_df['pd_type'].apply(lambda t: t.numpy_dtype)
    type_df['min_value'] = type_df['np_type'].apply(lambda row: np.iinfo(row).min)
    type_df['max_value'] = type_df['np_type'].apply(lambda row: np.iinfo(row).max)
    type_df['allow_negatives'] = type_df['min_value'] < 0
    type_df['size'] = type_df['np_type'].apply(lambda row: row.itemsize)
    type_df.sort_values(by=['size', 'allow_negatives'], inplace=True)
    return type_df.reset_index(drop=True)

def downcast_int(file_path, column:str, chunksize=100000, delimiter=',', signed=True, unsigned=True):
    '''Automatically downcast Number dtype for minimal possible'''
    types = get_types(signed, unsigned)
    negatives = False
    for chunk in pd.read_csv(file_path, usecols=[column],delimiter=delimiter,chunksize=chunksize):
        M = chunk[column].max()
        m = chunk[column].min()
        if not signed and not negatives and m < 0 :
            types = types[types['allow_negatives']] # removes unsigned rows
            negatives = True
        if m < types['min_value'].iloc[0]:
            types = types[types['min_value'] < m]
        if M > types['max_value'].iloc[0]:
            types = types[types['max_value'] > M]
        if len(types) == 1:
            print('early stop')
            break
    return types['pd_type'].iloc[0]

# End of functions based on the above link

# The follwing function is AI generated
def analyze_dataframe_for_optimization(
    df: pd.DataFrame,
    cat_threshold: float = 0.00001,          # Fraction of unique values vs n_rows to auto-categorize
    cat_max_unique: int = 75,             # Or absolute unique values for category suggestion
    downcast_float: bool = True,          # Whether to try float64 → float32
    require_all_float_notnull: bool = True, # Only downcast floats if all notnull (avoids infs/NANs mishaps)
    verbose: bool = False                 # Print suggestions as you go
) -> Dict[str, List[str]]:
    """
    Analyze a DataFrame and suggest optimized dtypes per column.
    Designed for auditability and flexible application via optimize_df_types.

    Returns a dictionary mapping dtype string → list of column names.
    """
    type_map: Dict[str, List[str]] = {}
    n_rows = len(df)

    for col in df.columns:
        s = df[col]
        orig_dtype = s.dtype

        # =================== INTEGER COLUMNS ==========================
        if pd.api.types.is_integer_dtype(orig_dtype):
            min_val, max_val = s.min(), s.max()
            has_na = s.isnull().any()

            # For nullable ints, use pandas extension dtypes (Int64, UInt32, etc)
            if has_na:
                # Pandas nullable dtypes require pandas >= 1.0
                # Choose signed or unsigned
                if min_val >= 0:
                    for dtype in ['UInt8', 'UInt16', 'UInt32', 'UInt64']:
                        # Use numpy's iinfo on raw dtype (strip capital 'U')
                        if max_val <= np.iinfo(dtype[1:].lower()).max:
                            rec = dtype
                            break
                    else:
                        rec = 'UInt64'
                else:
                    for dtype in ['Int8', 'Int16', 'Int32', 'Int64']:
                        if min_val >= np.iinfo(dtype[1:].lower()).min and max_val <= np.iinfo(dtype[1:].lower()).max:
                            rec = dtype
                            break
                    else:
                        rec = 'Int64'
            else:
                # No NA, can use numpy int types
                if min_val >= 0:
                    if max_val <= np.iinfo(np.uint8).max:
                        rec = 'uint8'
                    elif max_val <= np.iinfo(np.uint16).max:
                        rec = 'uint16'
                    elif max_val <= np.iinfo(np.uint32).max:
                        rec = 'uint32'
                    else:
                        rec = 'uint64'
                else:
                    if min_val >= np.iinfo(np.int8).min and max_val <= np.iinfo(np.int8).max:
                        rec = 'int8'
                    elif min_val >= np.iinfo(np.int16).min and max_val <= np.iinfo(np.int16).max:
                        rec = 'int16'
                    elif min_val >= np.iinfo(np.int32).min and max_val <= np.iinfo(np.int32).max:
                        rec = 'int32'
                    else:
                        rec = 'int64'
            type_map.setdefault(rec, []).append(col)
            if verbose:
                print(f"[{col}] Integer: min={min_val} max={max_val} "
                      f"nulls={has_na} → {rec}")

        # =================== FLOAT COLUMNS ============================
        elif pd.api.types.is_float_dtype(orig_dtype):
            rec = 'float64' # Default is keep as-is
            cur_min, cur_max = s.min(), s.max()

            if downcast_float:
                # Downcast safely if all notnull or if user allows
                if (not require_all_float_notnull) or s.notnull().all():
                    # Check if values fit in float32 range
                    if cur_min >= np.finfo(np.float32).min and cur_max <= np.finfo(np.float32).max:
                        rec = 'float32'
            type_map.setdefault(rec, []).append(col)
            if verbose:
                print(f"[{col}] Float: min={cur_min} max={cur_max} "
                      f"→ {rec}")

        # =================== OBJECT/STRING COLUMNS ====================
        elif orig_dtype == 'object':
            n_unique = s.nunique(dropna=False)
            # Use inferred type to help distinguish string-like columns from others (ignore mixed types here)
            # Suggest 'category' if cardinality below threshold, else 'string'
            if (n_unique <= cat_max_unique) or (n_unique / n_rows <= cat_threshold):
                rec = 'category'
            else:
                rec = 'string'
            type_map.setdefault(rec, []).append(col)
            if verbose:
                print(f"[{col}] Object: unique={n_unique} rows={n_rows} "
                      f"frac={n_unique/n_rows:.3f} → {rec}")

        # =================== BOOL COLUMNS =============================
        elif pd.api.types.is_bool_dtype(orig_dtype):
            # Already optimal
            continue

        # =================== DATETIME COLUMNS ==========================
        elif pd.api.types.is_datetime64_any_dtype(orig_dtype):
            # Leave as-is (already compact and speedy)
            continue

        # =================== OTHER TYPES ===============================
        else:
            # Extend as needed for timedelta, sparse, etc.
            if verbose:
                print(f"[{col}] dtype {orig_dtype} not handled for optimization (left as is)")

    return type_map

def df_memory_usage(df: pd.DataFrame) -> pd.Series:
    """
    Returns Series: memory usage (bytes) per column, with a 'Total' sum at the end.
    """
    mem = df.memory_usage(deep=True)
    mem['Total'] = mem.sum()
    return mem

def print_optimization_report(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    mapping: dict,
    show_missing: bool = True
):
    """
    Print a detailed before/after optimization report, including:
    - Per-column memory usage changes
    - Per-column dtype changes
    - Overall memory footprint change
    - Missing value counts (optional)
    - Type/column assignment summary

    Args:
        before_df: DataFrame before optimization
        after_df:  DataFrame after optimization
        mapping:   Dict mapping dtype → list of columns converted
        show_missing: Whether to display missing value counts before optimization
    """
    print("="*55)
    print("DataFrame Memory Usage Optimization Report")
    print("="*55)

    # --- (1) Memory usage per column ---
    before_mem = df_memory_usage(before_df)
    after_mem = df_memory_usage(after_df)
    mem_diff = before_mem - after_mem

    # --- (2) Type info ---
    # Get dtypes for "Total" row; use "" instead of dtype for "Total"
    before_dtypes = pd.concat([before_df.dtypes.astype(str), pd.Series({'Total': ""})])
    after_dtypes = pd.concat([after_df.dtypes.astype(str), pd.Series({'Total': ""})])

    mem_df = pd.DataFrame({
        "Before (bytes)": before_mem,
        "After (bytes)": after_mem,
        "Delta (bytes)": mem_diff,
        "Before dtype": before_dtypes,
        "After dtype": after_dtypes,
    })

    print("\n---- Per-column memory usage and dtype ----")
    print(mem_df)

    # --- (3) Overall % and total KB saved ---
    percent_decrease = 100 * (mem_diff['Total'] / before_mem['Total']) if before_mem['Total'] > 0 else 0
    print("\n--> Overall memory decreased by {0:.2f}%: {1:,} → {2:,} bytes ({3:,.0f} KB saved).\n"
          .format(percent_decrease, int(before_mem['Total']), int(after_mem['Total']), mem_diff['Total']/1024))

    # --- (4) Missing values report ---
    if show_missing:
        print("---- Missing Values (before optimization) ----")
        print(before_df.isnull().sum(), "\n")

    # --- (5) Type conversion summary ---
    print("---- Type conversions applied ----")
    for dtype, cols in mapping.items():
        for col in cols:
            before = before_df[col].dtype if col in before_df.columns else "N/A"
            after = after_df[col].dtype if col in after_df.columns else "N/A"
            print(f"{col:<25}: {before}  →  {dtype} (now: {after})")

    print("="*55 + "\n")
    print("Optimization report completed successfully.\n")
# End of AI generated functions

# Custom made function to determine the smallest safe integer type for a pandas Series
def get_safe_int_type(series: pd.Series) -> str:
    """Determine the smallest safe integer type for a series"""

    if type(series) != pd.Series:
        raise TypeError("Expected a pandas Series")

    min_val = series.min()
    max_val = series.max()

    # Check ranges for different integer types
    if min_val >= 0:  # Unsigned types
        if max_val <= 255:
            return 'uint8'
        elif max_val <= 65535:
            return 'uint16'
        elif max_val <= 4294967295:
            return 'uint32'
        else:
            return 'uint64'
    else:  # Signed types
        if min_val >= -128 and max_val <= 127:
            return 'int8'
        elif min_val >= -32768 and max_val <= 32767:
            return 'int16'
        elif min_val >= -2147483648 and max_val <= 2147483647:
            return 'int32'
        else:
            return 'int64'

def optimize_df_types(df: pd.DataFrame, df_types: dict, copy=True) -> pd.DataFrame:
    """
    Optimize memory usage of DataFrame based on provided types using CleanFlow.
    """
    import cleanflow
    
    if df_types is None:
        return df

    target_df = df.copy() if copy else df
    return cleanflow.apply_optimization(target_df, df_types)


def optimize_df_types_old( df: pd.DataFrame, df_types: dict) -> pd.DataFrame:
    """
    Optimize memory usage of DataFrame based on provided types.
    Works with pandas DataFrames.
    """
    if df_types is None:
        return df

    df_optimized = df.copy()
    df_columns = df_optimized.columns

    for dtype, columns in df_types.items():
        for col in columns:
            if col in df_columns:
                df_optimized[col] = df_optimized[col].astype(dtype)
            else:
                print(f"Warning: Column '{col}' not found in DataFrame")

    return df_optimized

def get_missing_values(df: pd.DataFrame) -> pd.Series:
    """Get the number of missing values in each column of a DataFrame"""
    if type(df) != pd.DataFrame:
        raise TypeError("Expected a pandas DataFrame")
    return df.isnull().sum()

def check_primary_key_candidates(df: pd.DataFrame, columns: list) -> dict:
    """
    Check if specified columns can serve as primary keys (unique, non-null).

    Args:
        df: DataFrame to check
        columns: List of column names to evaluate as primary key candidates

    Returns:
        dict: Results for each column with uniqueness and null information
    """
    # Input validation
    if type(df) != pd.DataFrame:
        raise TypeError("Expected a pandas DataFrame")
    if type(columns) != list:
        raise TypeError("Expected a list of column names")
    if not all(isinstance(col, str) for col in columns):
        raise TypeError("Column names must be strings")

    results = {}
    df_columns = df.columns
    # Check each column
    for col in columns:
        if col not in df_columns:
            results[col] = {
                'status': 'COLUMN_NOT_FOUND',
                'unique_count': 0,
                'total_count': 0,
                'null_count': 0,
                'duplicate_count': 0,
                'sample_duplicates': {}
            }
            continue

        total_count = len(df)
        unique_count = df[col].nunique()
        null_count = df[col].isnull().sum()
        duplicate_count = total_count - unique_count

        # Determine if it can be a primary key
        if null_count > 0:
            status = 'HAS_NULLS'
        elif duplicate_count > 0:
            status = 'HAS_DUPLICATES'
        else:
            status = 'VALID_PRIMARY_KEY'

        # Get sample duplicates if there are any (and not too many)
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.duplicated.html#pandas.Series.duplicated
        sample_duplicates = {}
        if status == 'HAS_DUPLICATES' and duplicate_count <= 10:
            duplicates = df[df[col].duplicated(keep=False)][col].value_counts().head(5) # Get the top 5 duplicates
            sample_duplicates = duplicates.to_dict()

        results[col] = {
            'status': status,
            'unique_count': unique_count,
            'total_count': total_count,
            'null_count': null_count,
            'duplicate_count': duplicate_count,
            'sample_duplicates': sample_duplicates
        }

    return results

def display_primary_key_analysis(pk_results: dict) -> None:
    """
    Display detailed primary key analysis results.

    Args:
        pk_results: Results from check_primary_key_candidates function
    """
    for col, result in pk_results.items():
        if result['status'] == 'COLUMN_NOT_FOUND':
            print(f"\nPrimary Key Analysis for '{col}':")
            print(f"  Column not found in DataFrame")
            continue

        print(f"\nPrimary Key Analysis for '{col}':")
        print(f"  Total rows: {result['total_count']:,}")
        print(f"  Unique values: {result['unique_count']:,}")
        print(f"  Null values: {result['null_count']:,}")
        print(f"  Duplicate values: {result['duplicate_count']:,}")
        print(f"  Status: {result['status']}")

        # Show sample duplicates if available
        if result['sample_duplicates']:
            print(f"  Sample duplicates:")
            for value, count in result['sample_duplicates'].items():
                print(f"    '{value}': appears {count} times")

def process_and_merge_files(file_list: list, optimization_types: dict, delimiter: str = '|') -> pd.DataFrame:
    """
    Process multiple CSV files by optimizing data types and merging them.

    Args:
        file_list: List of file paths to process
        optimization_types: Dictionary with data types and columns to optimize
        delimiter: CSV delimiter (default: '|')

    Returns:
        pd.DataFrame: Merged and optimized DataFrame
    """
    optimized_chunks = []
    total_rows = 0

    for i, file_path in enumerate(file_list):
        print(f"Processing file {i+1}/{len(file_list)}: {file_path}")

        # Load and optimize
        chunk_df = pd.read_csv(file_path, delimiter=delimiter)
        chunk_df = optimize_df_types(chunk_df, optimization_types)

        optimized_chunks.append(chunk_df)
        total_rows += len(chunk_df)

        if i % 10 == 0:  # Progress update every 10 files
            print(f"Processed {total_rows:,} rows so far...")

    # Merge all chunks
    print("Merging all optimized files...")
    merged_df = pd.concat(optimized_chunks, ignore_index=True)

    # Cleanup to free up memory
    del optimized_chunks
    import gc
    gc.collect()

    return merged_df

def create_fraud_detection_db(customers_df: pd.DataFrame, transactions_df: pd.DataFrame) -> None:
    """
    Create fraud detection database with proper primary keys and foreign keys.

    Args:
        customers_df: Customer DataFrame
        transactions_df: Transaction DataFrame
    """
    conn = sqlite3.connect('fraud_detection.db')
    cursor = conn.cursor()

    try:
        cursor.execute('PRAGMA foreign_keys = OFF')
        cursor.execute('DROP TABLE IF EXISTS transactions')
        cursor.execute('DROP TABLE IF EXISTS customers')
        cursor.execute('''
            CREATE TABLE customers (
                ssn TEXT PRIMARY KEY,
                cc_num TEXT,
                first TEXT,
                last TEXT,
                gender TEXT,
                street TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                lat REAL,
                long REAL,
                city_pop INTEGER,
                job TEXT,
                dob TEXT,
                acct_num TEXT,
                pop_group TEXT,
                location TEXT
            )
        ''')


        cursor.execute('''
            CREATE TABLE transactions (
                trans_num TEXT PRIMARY KEY,
                ssn TEXT,
                trans_date TEXT,
                trans_time TEXT,
                unix_time INTEGER,
                category TEXT,
                amt REAL,
                is_fraud INTEGER,
                merchant TEXT,
                merch_lat REAL,
                merch_long REAL,
                FOREIGN KEY (ssn) REFERENCES customers (ssn)
            )
        ''')

        customers_df.to_sql('customers', conn, if_exists='append', index=False)
        transactions_df.to_sql('transactions', conn, if_exists='append', index=False)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_ssn ON transactions(ssn)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_unix_time ON transactions(unix_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions(is_fraud)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_customers_state ON customers(state)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_customers_pop_group ON customers(pop_group)')

        cursor.execute('PRAGMA foreign_keys = ON')

        conn.commit()
        print("Database created successfully with proper primary keys, foreign keys, and indexes")

    except Exception as e:
        print(f"Error creating database: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def calculate_distance(lat1: Union[float, pd.Series, np.ndarray],
                      lon1: Union[float, pd.Series, np.ndarray],
                      lat2: Union[float, pd.Series, np.ndarray],
                      lon2: Union[float, pd.Series, np.ndarray]) -> Union[float, np.ndarray]:
    """
    Calculate the great circle distance between two points on Earth using the Haversine formula.

    Args:
        lat1: Latitude of first point(s) in decimal degrees (float, Series, or array)
        lon1: Longitude of first point(s) in decimal degrees (float, Series, or array)
        lat2: Latitude of second point(s) in decimal degrees (float, Series, or array)
        lon2: Longitude of second point(s) in decimal degrees (float, Series, or array)

    Returns:
        Distance between the points in kilometers (float if inputs are scalars, array if inputs are Series/arrays)

    Example:
        >>> distance = calculate_distance(40.7128, -74.0060, 34.0522, -118.2437)
        >>> print(f"Distance: {distance:.2f} km")
        Distance: 3944.42 km
    """

    # Convert to numpy arrays to handle both Series and individual values
    lat1, lon1, lat2, lon2 = map(np.asarray, [lat1, lon1, lat2, lon2])

    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    # Earth's radius in kilometers
    earth_radius_km = 6371

    return c * earth_radius_km

if __name__ == "__main__":

    print("Useful functions loaded successfully")
