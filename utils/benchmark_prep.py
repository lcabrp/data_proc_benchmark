"""Shared benchmark preparation helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from utils.useful_functions import optimize_df_types

PREP_COLUMNS = [
    "prep_pandas_seconds",
    "prep_polars_seconds",
    "prep_duckdb_seconds",
    "prep_fireducks_seconds",
]

_prep_timings: Dict[str, Dict[str, float]] = {}


def reset_prep_timings() -> None:
    """Clear prep timing totals for a benchmark run."""
    _prep_timings.clear()


def record_prep_timing(library: str, step: str, elapsed: float) -> None:
    """Record a preparation timing step."""
    lib = library.lower()
    _prep_timings.setdefault(lib, {})[step] = elapsed


def print_prep_timing(library: str, step: str, start_time: float) -> float:
    """Print and record elapsed prep time."""
    elapsed = time.perf_counter() - start_time
    record_prep_timing(library, step, elapsed)
    print(f"  [{library} prep] {step}: {elapsed:.3f}s")
    return elapsed


def get_prep_seconds(library: str) -> Optional[float]:
    """Return total prep seconds for a library when available."""
    steps = _prep_timings.get(library.lower(), {})
    for key in (
        "total load/optimization",
        "total before optimization failure",
        "total before failure",
    ):
        if key in steps:
            return steps[key]
    return None


def get_prep_csv_values() -> list[Optional[float]]:
    """Return prep timing values ordered for PREP_COLUMNS."""
    return [
        get_prep_seconds("pandas"),
        get_prep_seconds("polars"),
        get_prep_seconds("duckdb"),
        get_prep_seconds("fireducks"),
    ]


def memory_usage_bytes(df: pd.DataFrame, mode: str) -> Optional[int]:
    """Return DataFrame memory usage according to the selected report mode."""
    if mode == "off":
        return None
    if mode == "shallow":
        return int(df.memory_usage(deep=False).sum())
    if mode == "deep":
        return int(df.memory_usage(deep=True).sum())
    raise ValueError(f"Unsupported prep memory report mode: {mode}")


def csv_read_kwargs_for_types(type_map: dict) -> dict:
    """Build pandas read_csv kwargs from benchmark dtype optimization rules."""
    dtype: Dict[str, str] = {}
    parse_dates: list[str] = []
    for target_dtype, columns in type_map.items():
        if target_dtype == "datetime64[ns]":
            parse_dates.extend(columns)
        else:
            for col in columns:
                dtype[col] = target_dtype

    kwargs: Dict[str, Any] = {}
    if dtype:
        kwargs["dtype"] = dtype
    if parse_dates:
        kwargs["parse_dates"] = parse_dates
    return kwargs


def _read_with_pandas_like(
    source_path: Path,
    *,
    library: str,
    type_map: dict,
    use_dtype_hints: bool,
) -> pd.DataFrame:
    if library == "fireducks":
        import fireducks.pandas as pd_like
    else:
        pd_like = pd

    suffix = source_path.suffix.lower()
    if suffix == ".parquet":
        return pd_like.read_parquet(source_path)
    if suffix in (".jsonl", ".ndjson"):
        return pd_like.read_json(source_path, lines=True)
    if suffix == ".json":
        return pd_like.read_json(source_path)

    kwargs = csv_read_kwargs_for_types(type_map) if use_dtype_hints else {}
    try:
        return pd_like.read_csv(source_path, **kwargs)
    except Exception as exc:
        if kwargs:
            print(f"  {library} dtype-hinted CSV read failed ({exc}); retrying without dtype hints")
            return pd_like.read_csv(source_path)
        raise


def _cache_key(source_path: Path, type_map: dict) -> str:
    stat = source_path.stat()
    payload = {
        "path": str(source_path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "type_map": type_map,
    }
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:12]


def optimized_cache_path(source_path: Path, cache_dir: Path, type_map: dict) -> Path:
    """Return the cache path for an optimized pandas/fireducks dataset."""
    source_kind = source_path.suffix.lower().lstrip(".") or "data"
    name = f"{source_path.stem}.{source_kind}.optimized.{_cache_key(source_path, type_map)}.parquet"
    return cache_dir / name


def load_pandas_like_for_benchmark(
    source_path: Path,
    *,
    library: str,
    type_map: dict,
    should_optimize: bool,
    prep_memory_report: str,
    optimized_cache_mode: str,
    optimized_cache_dir: Path,
    use_dtype_hints: bool = True,
) -> pd.DataFrame:
    """Load, optionally optimize, and optionally cache a pandas-like DataFrame."""
    prep_start = time.perf_counter()
    cache_path = optimized_cache_path(source_path, optimized_cache_dir, type_map)
    can_read_cache = optimized_cache_mode in {"read", "readwrite"} and cache_path.exists()

    if can_read_cache:
        step_start = time.perf_counter()
        df = _read_with_pandas_like(
            cache_path,
            library=library,
            type_map=type_map,
            use_dtype_hints=False,
        )
        print_prep_timing(library, "read/load optimized cache", step_start)
        print(f"  {library} loaded optimized cache: {cache_path}")
        print_prep_timing(library, "total load/optimization", prep_start)
        return df

    step_start = time.perf_counter()
    df = _read_with_pandas_like(
        source_path,
        library=library,
        type_map=type_map,
        use_dtype_hints=use_dtype_hints,
    )
    print_prep_timing(library, "read/load", step_start)

    if not should_optimize:
        print(f"  {library} DataFrame loaded without memory optimization")
        print_prep_timing(library, "total load/optimization", prep_start)
        return df

    original_memory = None
    if prep_memory_report != "off":
        step_start = time.perf_counter()
        original_memory = memory_usage_bytes(df, prep_memory_report)
        print_prep_timing(library, f"{prep_memory_report} memory before optimization", step_start)

    step_start = time.perf_counter()
    optimized_df = optimize_df_types(df, type_map, False)
    print_prep_timing(library, "dtype optimization", step_start)

    optimized_memory = None
    if prep_memory_report != "off":
        step_start = time.perf_counter()
        optimized_memory = memory_usage_bytes(optimized_df, prep_memory_report)
        print_prep_timing(library, f"{prep_memory_report} memory after optimization", step_start)

    if original_memory and optimized_memory:
        memory_reduction = (original_memory - optimized_memory) / original_memory * 100
        if memory_reduction > 1:
            print(
                f"  {library} DataFrame optimized: {memory_reduction:.1f}% memory reduction "
                f"({original_memory / 1024 / 1024:.1f}MB -> {optimized_memory / 1024 / 1024:.1f}MB)"
            )
    else:
        print(f"  {library} DataFrame optimized")

    if optimized_cache_mode in {"write", "readwrite", "refresh"}:
        optimized_cache_dir.mkdir(parents=True, exist_ok=True)
        step_start = time.perf_counter()
        optimized_df.to_parquet(cache_path, index=False)
        print_prep_timing(library, "write optimized cache", step_start)
        print(f"  {library} wrote optimized cache: {cache_path}")

    print_prep_timing(library, "total load/optimization", prep_start)
    return optimized_df


def append_csv_row_with_schema(path: Path, header: Iterable[str], row: Iterable[Any]) -> None:
    """Append a CSV row, expanding existing headers when new columns are introduced."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    header_list = list(header)
    row_dict = {key: value for key, value in zip(header_list, row)}

    if not path.exists() or path.stat().st_size == 0:
        with open(path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=header_list)
            writer.writeheader()
            writer.writerow(row_dict)
        return

    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        existing_header = reader.fieldnames or []
        existing_rows = list(reader)

    merged_header = existing_header + [col for col in header_list if col not in existing_header]
    if merged_header != existing_header:
        with open(path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=merged_header, extrasaction="ignore")
            writer.writeheader()
            for existing in existing_rows:
                writer.writerow(existing)
            writer.writerow(row_dict)
        return

    with open(path, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=merged_header, extrasaction="ignore")
        writer.writerow(row_dict)
