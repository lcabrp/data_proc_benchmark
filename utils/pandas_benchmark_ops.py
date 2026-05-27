"""Optimized pandas-like benchmark operations shared by benchmark scripts."""

from __future__ import annotations

import warnings
from typing import Optional

import pandas as pd


def _groupby_sum_by_source(df: pd.DataFrame, observed: bool) -> pd.Series:
    try:
        return df.groupby("source_ip", observed=observed)["bytes"].transform("sum")
    except TypeError:
        return df.groupby("source_ip")["bytes"].transform("sum")


def _rank_by_event_type(total_bytes: pd.Series, event_type: pd.Series, observed: bool) -> pd.Series:
    try:
        return total_bytes.groupby(event_type, observed=observed).rank(
            method="dense",
            ascending=False,
        )
    except TypeError:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="The default of observed=False is deprecated",
                category=FutureWarning,
            )
            return total_bytes.groupby(event_type).rank(method="dense", ascending=False)


def complex_join_top_ranked(
    df: pd.DataFrame,
    *,
    rank_col: str = "total_rank",
    observed: bool = False,
    sort_by_rank: bool = False,
) -> Optional[pd.DataFrame]:
    """Compute the full-row complex-join benchmark without a wide merge allocation."""
    req = {"source_ip", "bytes", "event_type"}
    if not req.issubset(df.columns):
        return None

    try:
        total_bytes = _groupby_sum_by_source(df, observed)
        rank = _rank_by_event_type(total_bytes, df["event_type"], observed)
        mask = rank <= 10
        result = df.loc[mask].copy()
        result["total_bytes"] = total_bytes.loc[mask]
        result[rank_col] = rank.loc[mask]
    except Exception:
        summary = (
            df.groupby("source_ip", observed=observed)["bytes"]
            .sum()
            .reset_index()
            .rename(columns={"bytes": "total_bytes"})
        )
        merged = df.merge(summary, on="source_ip", how="left")
        merged[rank_col] = _rank_by_event_type(merged["total_bytes"], merged["event_type"], observed)
        result = merged.loc[merged[rank_col] <= 10]

    if sort_by_rank:
        return result.sort_values(rank_col)
    return result


def _extract_hour_fast(timestamp: pd.Series):
    if pd.api.types.is_datetime64_any_dtype(timestamp):
        return timestamp.dt.hour

    try:
        hour_text = timestamp.str.slice(11, 13)
        return hour_text.astype("uint8")
    except Exception:
        return pd.to_datetime(timestamp, errors="coerce").dt.hour


def timeseries_hour_counts(
    df: pd.DataFrame,
    *,
    observed: bool = False,
    reset_index: bool = False,
    hour_name: str = "_hour",
) -> Optional[pd.Series | pd.DataFrame]:
    """Extract hour and count by hour/event_type without copying the full frame."""
    if "event_type" not in df.columns:
        return None

    if "timestamp" in df.columns:
        hour = _extract_hour_fast(df["timestamp"])
    else:
        hour = pd.Series(0, index=df.index, name=hour_name)

    if hasattr(hour, "rename"):
        hour = hour.rename(hour_name)

    try:
        result = df.groupby([hour, df["event_type"]], observed=observed).size()
    except TypeError:
        result = df.groupby([hour, df["event_type"]]).size()

    if reset_index:
        return result.reset_index(name="count")
    return result
