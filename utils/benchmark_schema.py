"""
Shared benchmark schema constants.

This module exists so that every benchmark runner uses the same single source
of truth for dtype optimization rules, operation ordering, and library ordering.
Without it, the four benchmark scripts duplicated ``BENCHMARK_OPTIMIZATION_TYPES``
and hard-coded their own operation/library lists, which made the suite harder to
maintain and easier to drift out of sync.
"""

from __future__ import annotations

# Column-level dtype optimization rules used when loading pandas-like libraries.
# These dtypes are chosen to minimize memory footprint while preserving the
# precision needed by the four benchmark operations.
BENCHMARK_OPTIMIZATION_TYPES = {
    "datetime64[ns]": ["timestamp"],
    "category": [
        "source_ip",
        "destination_ip",
        "protocol",
        "event_type",
        "severity",
        "user",
        "status_code",
        "country",
        "device_type",
    ],
    "uint32": ["bytes", "session_id"],
    "uint16": ["response_time_ms", "port"],
    "float32": ["risk_score"],
}

# Canonical operation ordering. This order is used for execution, for CSV column
# layout, and for the summary report. New operations should be appended here.
OPERATION_ORDER = [
    "filter_group",
    "statistics",
    "complex_join",
    "timeseries",
]

# Canonical library ordering. Pandas is first because it is the baseline; other
# libraries are ordered by typical execution speed for easy reading.
LIBRARY_ORDER = [
    "pandas",
    "polars",
    "duckdb",
    "fireducks",
]
