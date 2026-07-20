"""Shared benchmark preparation helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd

PREP_COLUMNS = [
    "prep_pandas_seconds",
    "prep_polars_seconds",
    "prep_duckdb_seconds",
    "prep_fireducks_seconds",
]

_prep_timings: Dict[str, Dict[str, float]] = {}


@dataclass(frozen=True)
class MemoryOptimizationDecision:
    """Decision and display text for pandas-like dtype optimization."""

    should_optimize: bool
    total_memory_gb: Optional[float]
    message: str


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


def decide_memory_optimization(
    optimize_mode: str,
    memory_threshold_gb: float,
) -> MemoryOptimizationDecision:
    """Resolve auto/always/never memory optimization behavior in one place."""
    if optimize_mode not in {"auto", "always", "never"}:
        raise ValueError(f"Unsupported optimization mode: {optimize_mode}")

    total_memory_gb: Optional[float]
    try:
        import psutil

        total_memory_gb = psutil.virtual_memory().total / (1024**3)
    except Exception:
        total_memory_gb = None

    memory_label = (
        f"{total_memory_gb:.1f}GB RAM"
        if total_memory_gb is not None
        else "unknown system memory"
    )

    if optimize_mode == "always":
        return MemoryOptimizationDecision(
            True,
            total_memory_gb,
            f"System has {memory_label} - optimization FORCED via --optimize always",
        )

    if optimize_mode == "never":
        return MemoryOptimizationDecision(
            False,
            total_memory_gb,
            f"System has {memory_label} - optimization DISABLED via --optimize never",
        )

    if total_memory_gb is None:
        return MemoryOptimizationDecision(
            True,
            None,
            "Could not determine system memory - applying optimization for safety",
        )

    if total_memory_gb < memory_threshold_gb:
        return MemoryOptimizationDecision(
            True,
            total_memory_gb,
            f"System has {total_memory_gb:.1f}GB RAM (< {memory_threshold_gb}GB threshold) - applying optimization",
        )

    return MemoryOptimizationDecision(
        False,
        total_memory_gb,
        f"System has {total_memory_gb:.1f}GB RAM (>= {memory_threshold_gb}GB threshold) - skipping optimization",
    )


def build_script_name(
    base_name: str,
    optimize_mode: str,
    optimization_applied: bool,
    total_memory_gb: Optional[float] = None,
) -> str:
    """Build a self-documenting ``script_name`` that encodes the optimize mode.

    Produces values such as ``benchmark.py_opt_always``, ``benchmark_02.py_opt_never``
    or ``benchmark_modular.py_opt_auto_mem32GB`` so the optimization state of each
    run is recoverable directly from the results CSV. All four benchmark runners
    share this single implementation to prevent label drift between scripts.

    Args:
        base_name: The bare script filename (e.g. ``"benchmark_02.py"``).
        optimize_mode: One of ``"auto"``, ``"always"`` or ``"never"``.
        optimization_applied: Whether dtype optimization was actually applied.
            Only affects the ``auto`` mode suffix.
        total_memory_gb: System RAM in GB; queried via psutil when ``None``.
            Only used to annotate the ``auto`` mode suffix.

    Returns:
        The base name with an optimization suffix appended, e.g.
        ``"benchmark.py_opt_always"``. Falls back to ``f"{base_name}_opt_unknown"``
        when system memory cannot be determined.
    """
    try:
        if total_memory_gb is None:
            import psutil

            total_memory_gb = psutil.virtual_memory().total / (1024**3)
        if optimize_mode == "always":
            opt_info = "opt_always"
        elif optimize_mode == "never":
            opt_info = "opt_never"
        elif optimization_applied:
            opt_info = f"opt_auto_mem{total_memory_gb:.0f}GB"
        else:
            opt_info = f"no_opt_auto_mem{total_memory_gb:.0f}GB"
        return f"{base_name}_{opt_info}"
    except Exception:
        return f"{base_name}_opt_unknown"


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
    """Load, optionally optimize, and optionally cache a pandas-like DataFrame using CleanFlow."""
    import cleanflow
    
    prep_start = time.perf_counter()
    
    # Caching check
    use_cache = optimized_cache_mode in {"read", "readwrite", "refresh"}
    
    # Load dataset using CleanFlow's high-performance loader with read-time dtypes and transparent cache
    df = cleanflow.io.load_dataset(
        source_path,
        engine="pandas" if library in {"pandas", "fireducks"} else library,
        type_map=type_map if should_optimize else None,
        use_dtype_hints=use_dtype_hints,
        cache=use_cache,
        cache_dir=optimized_cache_dir,
    )
    
    # FireDucks handling
    if library == "fireducks":
        import fireducks.pandas as fpd
        if not isinstance(df, fpd.DataFrame):
            df = fpd.DataFrame(df)
            
    print_prep_timing(library, "total load/optimization", prep_start)
    return df


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
