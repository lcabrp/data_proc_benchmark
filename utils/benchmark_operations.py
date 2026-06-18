"""
Shared benchmark operation implementations (strategy pattern).

Each operation in this module encapsulates the same logical work expressed in
pandas, Polars, DuckDB SQL, and FireDucks. Centralizing the implementations
removes the duplication of 16 functions (4 operations x 4 libraries) that was
repeated across benchmark.py, benchmark_01.py, benchmark_02.py, and
benchmark_modular.py.

The operation classes are intentionally parameterized so that each benchmark
script can keep its historical quirks (e.g., ``rank_col="bytes_rank"`` in
benchmark.py vs ``rank_col="total_rank"`` in the others) without forking the
logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from utils.pandas_benchmark_ops import complex_join_top_ranked, timeseries_hour_counts

# Optional libraries are imported defensively. Type annotations are kept as
# strings where needed so this module imports even when polars/duckdb are not
# installed.
try:
    import polars as pl
except ImportError:
    pl = None  # type: ignore[assignment]

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore[assignment]


class BenchmarkOperation(ABC):
    """
    Abstract strategy for one benchmark operation across all libraries.

    Concrete operations implement the same logical work using each library's
    idioms. FireDucks reuses the pandas implementation by default because it
    exposes a pandas-compatible API.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short operation name used as a dictionary key and CSV column base."""

    @abstractmethod
    def run_pandas(self, df: pd.DataFrame) -> Any:
        """Execute the operation using pandas."""

    @abstractmethod
    def run_polars(self, df: pl.DataFrame) -> Any:
        """Execute the operation using Polars."""

    @abstractmethod
    def run_duckdb(self, expr: str, con: "duckdb.DuckDBPyConnection") -> Any:
        """Execute the operation using DuckDB SQL."""

    def run_fireducks(self, df: pd.DataFrame) -> Any:
        """
        Execute the operation using FireDucks.

        FireDucks is designed as a drop-in pandas replacement, so the default
        implementation delegates to the pandas path. Override only if a
        FireDucks-specific optimization is needed.
        """
        return self.run_pandas(df)


class FilterGroupOperation(BenchmarkOperation):
    """Filter rows where bytes > 1000, then group by event_type and count."""

    name = "filter_group"

    def __init__(self, observed: bool = False) -> None:
        self.observed = observed

    def run_pandas(self, df: pd.DataFrame) -> Any:
        if "bytes" not in df.columns or "event_type" not in df.columns:
            return None
        filtered = df[df["bytes"] > 1000]
        return filtered.groupby("event_type", observed=self.observed).size()

    def run_polars(self, df: pl.DataFrame) -> Any:
        if not {"bytes", "event_type"}.issubset(set(df.columns)):
            return None
        return df.filter(pl.col("bytes") > 1000).group_by("event_type").agg(
            pl.len().alias("count")
        )

    def run_duckdb(self, expr: str, con: "duckdb.DuckDBPyConnection") -> Any:
        return con.execute(
            f"""
            SELECT event_type, COUNT(*) AS count
            FROM {expr}
            WHERE bytes > 1000
            GROUP BY event_type
            """
        ).fetch_arrow_table()


class StatisticsOperation(BenchmarkOperation):
    """Group by event_type and compute mean/min/max for selected columns."""

    name = "statistics"

    def __init__(
        self,
        observed: bool = False,
        stat_cols: tuple[str, ...] = ("bytes", "response_time_ms", "risk_score"),
        filter_numeric_kinds: bool = False,
    ) -> None:
        self.observed = observed
        self.stat_cols = list(stat_cols)
        self.filter_numeric_kinds = filter_numeric_kinds

    def _available_cols(self, df: pd.DataFrame) -> list[str]:
        """Return the columns that exist and (optionally) are numeric."""
        available = [c for c in self.stat_cols if c in df.columns]
        if self.filter_numeric_kinds:
            available = [c for c in available if df[c].dtype.kind in "biufc"]
        return available

    def run_pandas(self, df: pd.DataFrame) -> Any:
        available = self._available_cols(df)
        if not available or "event_type" not in df.columns:
            return None
        return df.groupby("event_type", observed=self.observed).agg(
            {c: ["mean", "min", "max"] for c in available}
        )

    def run_polars(self, df: pl.DataFrame) -> Any:
        required = {"event_type", *self.stat_cols}
        if not required.issubset(set(df.columns)):
            return None
        aggs = []
        for c in self.stat_cols:
            aggs.extend(
                [
                    pl.col(c).mean().alias(f"{c}_mean"),
                    pl.col(c).min().alias(f"{c}_min"),
                    pl.col(c).max().alias(f"{c}_max"),
                ]
            )
        return df.group_by("event_type").agg(aggs)

    def run_duckdb(self, expr: str, con: "duckdb.DuckDBPyConnection") -> Any:
        cols_sql = ",\n                   ".join(
            f"AVG({c}) AS {c}_mean, MIN({c}) AS {c}_min, MAX({c}) AS {c}_max"
            for c in self.stat_cols
        )
        return con.execute(
            f"""
            SELECT event_type,
                   {cols_sql}
            FROM {expr}
            GROUP BY event_type
            """
        ).fetch_arrow_table()


class ComplexJoinOperation(BenchmarkOperation):
    """
    Sum bytes by source_ip, join back, rank by total_bytes within each
    event_type, and keep the top 10 rows.
    """

    name = "complex_join"

    def __init__(
        self,
        rank_col: str = "total_rank",
        sort_by_rank: bool = False,
        observed: bool = False,
    ) -> None:
        self.rank_col = rank_col
        self.sort_by_rank = sort_by_rank
        self.observed = observed

    def run_pandas(self, df: pd.DataFrame) -> Any:
        return complex_join_top_ranked(
            df,
            rank_col=self.rank_col,
            observed=self.observed,
            sort_by_rank=self.sort_by_rank,
        )

    def run_polars(self, df: pl.DataFrame) -> Any:
        if not {"source_ip", "bytes", "event_type"}.issubset(set(df.columns)):
            return None
        summary = df.group_by("source_ip").agg(
            pl.col("bytes").sum().alias("total_bytes")
        )
        joined = df.join(summary, on="source_ip", how="left")
        ranked = joined.with_columns(
            pl.col("total_bytes")
            .rank("dense", descending=True)
            .over("event_type")
            .alias(self.rank_col)
        )
        result = ranked.filter(pl.col(self.rank_col) <= 10)
        if self.sort_by_rank:
            result = result.sort(self.rank_col)
        return result

    def run_duckdb(self, expr: str, con: "duckdb.DuckDBPyConnection") -> Any:
        order_sql = f"ORDER BY {self.rank_col}" if self.sort_by_rank else ""
        return con.execute(
            f"""
            WITH summary AS (
                SELECT source_ip, SUM(bytes) AS total_bytes
                FROM {expr}
                GROUP BY source_ip
            ),
            ranked AS (
                SELECT d.*, s.total_bytes,
                       DENSE_RANK() OVER (
                           PARTITION BY d.event_type ORDER BY s.total_bytes DESC
                       ) AS {self.rank_col}
                FROM {expr} d
                JOIN summary s USING (source_ip)
            )
            SELECT * FROM ranked WHERE {self.rank_col} <= 10
            {order_sql}
            """
        ).fetch_arrow_table()


class TimeseriesOperation(BenchmarkOperation):
    """Extract hour from timestamp, group by (hour, event_type), and count."""

    name = "timeseries"

    def __init__(
        self,
        observed: bool = False,
        reset_index: bool = False,
        hour_name: str = "_hour",
    ) -> None:
        self.observed = observed
        self.reset_index = reset_index
        self.hour_name = hour_name

    def run_pandas(self, df: pd.DataFrame) -> Any:
        return timeseries_hour_counts(
            df,
            observed=self.observed,
            reset_index=self.reset_index,
            hour_name=self.hour_name,
        )

    def run_polars(self, df: pl.DataFrame) -> Any:
        if "event_type" not in df.columns:
            return None
        if "timestamp" in df.columns:
            if df["timestamp"].dtype == pl.Utf8:
                hour_expr = pl.col("timestamp").str.slice(11, 2).cast(pl.UInt8)
            else:
                hour_expr = pl.col("timestamp").dt.hour()
            df2 = df.with_columns(hour_expr.alias(self.hour_name))
        else:
            df2 = df.with_columns(pl.lit(0).alias(self.hour_name))
        return df2.group_by([self.hour_name, "event_type"]).agg(
            pl.len().alias("count")
        )

    def run_duckdb(self, expr: str, con: "duckdb.DuckDBPyConnection") -> Any:
        try:
            return con.execute(
                f"""
                SELECT EXTRACT(hour FROM CAST(timestamp AS TIMESTAMP)) AS hour,
                       event_type,
                       COUNT(*) AS count
                FROM {expr}
                GROUP BY hour, event_type
                """
            ).fetch_arrow_table()
        except Exception:
            # Fallback when ``timestamp`` is missing or not castable. The
            # operation semantics still require a per-event_type count, so we
            # use hour 0 as a neutral placeholder.
            return con.execute(
                f"""
                SELECT 0 AS hour, event_type, COUNT(*) AS count
                FROM {expr}
                GROUP BY event_type
                """
            ).fetch_arrow_table()
