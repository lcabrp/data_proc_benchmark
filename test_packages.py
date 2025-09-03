#!/usr/bin/env python3
"""Test script to check package availability in virtual environment"""

try:
    import pandas as pd
    print(f"✓ Pandas OK: {pd.__version__}")
except ImportError as e:
    print(f"✗ Pandas failed: {e}")

try:
    import modin.pandas as mpd
    print("✓ Modin OK")
except ImportError as e:
    print(f"✗ Modin failed: {e}")

try:
    import polars as pl
    print("✓ Polars OK")
except ImportError as e:
    print(f"✗ Polars failed: {e}")

try:
    import duckdb
    print("✓ DuckDB OK")
except ImportError as e:
    print(f"✗ DuckDB failed: {e}")

print("\nVirtual environment packages check complete.")
