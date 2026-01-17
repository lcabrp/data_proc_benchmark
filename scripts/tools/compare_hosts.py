"""Host-to-Host Benchmark Comparison Tool.

This module provides comprehensive comparison of benchmark performance between two hosts,
with statistical outlier detection and detailed analysis across operating systems, file
formats, and data processing libraries (pandas, polars, DuckDB, FireDucks).

Key Features:
    - Automatic outlier removal using IQR method (default, use --keep-outliers to disable)
    - Per-OS comparison (Windows, Linux, WSL2)
    - Per-format comparison (CSV, Parquet, JSON, NDJSON)
    - Per-library performance metrics (mean, median, percentiles, standard deviation)
    - JSON/NDJSON export with caching and reuse capabilities
    - Report reorientation for symmetric comparisons (compare_A_vs_B.json can be reused for B vs A)

Outlier Detection:
    Uses the Interquartile Range (IQR) method with 1.5x multiplier (Tukey's method):
    - Outliers = values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
    - Applied independently per library (pandas, polars, duckdb, fireducks)
    - Removes entire rows where any library shows outlier performance
    - Typically removes ~1-2% of data representing system anomalies (thermal throttling,
      background processes, early buggy script versions)

Usage:
    # Basic comparison with outlier removal (default)
    python compare_hosts.py --csv data/results.csv --host HostA --host HostB
    
    # Keep outliers (disable filtering)
    python compare_hosts.py --csv data/results.csv --host HostA --host HostB --keep-outliers
    
    # Export to JSON with specific libraries and formats
    python compare_hosts.py --csv data/results.csv --host HostA --host HostB \\
        --libs pandas,polars,duckdb --formats csv,parquet --json-out report.json

Example Output:
    ═══════════════════════════════════════════════════════════════════════════
                              WINNER: HostB is faster
    ═══════════════════════════════════════════════════════════════════════════
    
    OVERALL SUMMARY:
      HostA: 15.23s ±2.45 (mean ±stdev)
      HostB: 9.87s ±1.32 (mean ±stdev)
      HostB is 35.2% faster than HostA

Author: Data Processing Benchmark Project
Version: 1.0
Last Updated: January 2026
"""

import csv
import argparse
import sys
from pathlib import Path
from statistics import mean, median, stdev
from typing import Dict, List, Optional, Tuple
import difflib
from datetime import datetime, timezone
import os
import numpy as np

LIB_OPS = {
    'pandas': [
        'filter_group_pandas_seconds',
        'statistics_pandas_seconds',
        'complex_join_pandas_seconds',
        'timeseries_pandas_seconds',
    ],
    'polars': [
        'filter_group_polars_seconds',
        'statistics_polars_seconds',
        'complex_join_polars_seconds',
        'timeseries_polars_seconds',
    ],
    'duckdb': [
        'filter_group_duckdb_seconds',
        'statistics_duckdb_seconds',
        'complex_join_duckdb_seconds',
        'timeseries_duckdb_seconds',
    ],
    'fireducks': [
        'filter_group_fireducks_seconds',
        'statistics_fireducks_seconds',
        'complex_join_fireducks_seconds',
        'timeseries_fireducks_seconds',
    ],
}


def fval(x: Optional[str]) -> Optional[float]:
    """Convert string to float, handling None, empty strings, and 'N/A' values.
    
    Args:
        x: String value to convert, or None
        
    Returns:
        Float value if conversion succeeds, None otherwise
        
    Example:
        >>> fval("12.5")
        12.5
        >>> fval("N/A")
        None
    """
    try:
        return float(x) if x not in (None, "", "N/A") else None
    except Exception:
        return None


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    """Load all rows from benchmark results CSV file.
    
    Args:
        csv_path: Path to benchmark_results.csv file
        
    Returns:
        List of dictionaries, one per CSV row with column names as keys
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        csv.Error: If CSV file is malformed
    """
    with open(csv_path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        return list(rdr)


def remove_outliers_iqr(rows: List[Dict[str, str]], lib_ops: Dict[str, List[str]], multiplier: float = 1.5) -> Tuple[List[Dict[str, str]], int]:
    """
    Remove outlier rows using IQR method per library.
    For each row, compute mean time per library. Remove rows where any library's mean is an outlier.
    Returns: (filtered_rows, num_removed)
    """
    if len(rows) < 4:  # Need at least 4 rows for meaningful IQR
        return rows, 0
    
    # Compute per-row means for each library
    row_lib_means: Dict[str, List[Tuple[int, float]]] = {lib: [] for lib in lib_ops}
    
    for idx, r in enumerate(rows):
        for lib, cols in lib_ops.items():
            vals = [fval(r.get(c)) for c in cols]
            vals = [v for v in vals if v is not None]
            if vals:
                row_lib_means[lib].append((idx, mean(vals)))
    
    # Determine outlier rows using IQR method per library
    outlier_indices = set()
    for lib, data in row_lib_means.items():
        if len(data) < 4:
            continue
        
        values = [v for _, v in data]
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr
        
        for idx, val in data:
            if val < lower_bound or val > upper_bound:
                outlier_indices.add(idx)
    
    # Filter out outlier rows
    filtered = [r for i, r in enumerate(rows) if i not in outlier_indices]
    return filtered, len(outlier_indices)


def _norm_list(values: Optional[List[str]]) -> Optional[List[str]]:
    if not values:
        return None
    out: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip().lower()
        if not s:
            continue
        out.append(s)
    return sorted(list(dict.fromkeys(out)))


def _norm_libs_arg(libs: Optional[List[str]]) -> Optional[List[str]]:
    if libs is None:
        return None
    allow = set(LIB_OPS.keys())
    return [l for l in _norm_list(libs) or [] if l in allow] or None


def _filter_rows_by_host(rows: List[Dict[str, str]], host: str) -> List[Dict[str, str]]:
    return [r for r in rows if r.get('hostname') == host]


def _dataset_signature_for_host(rows: List[Dict[str, str]], host: str) -> Dict[str, Optional[str]]:
    host_rows = _filter_rows_by_host(rows, host)
    ts_vals = [r.get('timestamp') for r in host_rows if r.get('timestamp')]
    max_ts = max(ts_vals) if ts_vals else None
    return {
        'rows': len(host_rows),
        'max_timestamp': max_ts,
    }


def _file_stat_safe(path: Path) -> Dict[str, Optional[float]]:
    try:
        st = path.stat()
        return {
            'size_bytes': int(st.st_size),
            'mtime_epoch': float(st.st_mtime),
        }
    except Exception:
        return {
            'size_bytes': None,
            'mtime_epoch': None,
        }


def _build_meta(
    *,
    csv_path: Path,
    host_a: str,
    host_b: str,
    formats: Optional[List[str]],
    libs: Optional[List[str]],
    tie_threshold_pct: float,
    rows_effective: List[Dict[str, str]],
    source: str,
    source_report: Optional[str] = None,
) -> Dict:
    csv_stat = _file_stat_safe(csv_path)
    sig = {
        host_a: _dataset_signature_for_host(rows_effective, host_a),
        host_b: _dataset_signature_for_host(rows_effective, host_b),
    }
    return {
        'version': 1,
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'generated_via': source,
        'source_report': source_report,
        'csv': {
            'path': str(csv_path),
            **csv_stat,
        },
        'args': {
            'hosts': [host_a, host_b],
            'formats': _norm_list(formats),
            'libs': _norm_libs_arg(libs),
            'tie_threshold_pct': float(tie_threshold_pct),
        },
        # Signature is keyed by hostname so it is orientation-agnostic.
        'signature': sig,
    }


def _signature_matches(meta: Dict, current_sig: Dict[str, Dict[str, Optional[str]]]) -> bool:
    try:
        if meta.get('version') != 1:
            return False
        meta_sig = meta.get('signature') or {}
        # Require all hosts present.
        for host, sig in current_sig.items():
            if host not in meta_sig:
                return False
            if meta_sig.get(host) != sig:
                return False
        return True
    except Exception:
        return False


def _args_match(meta: Dict, formats: Optional[List[str]], libs: Optional[List[str]], tie_threshold_pct: float) -> bool:
    try:
        a = meta.get('args') or {}
        if _norm_list(a.get('formats')) != _norm_list(formats):
            return False
        if _norm_libs_arg(a.get('libs')) != _norm_libs_arg(libs):
            return False
        if float(a.get('tie_threshold_pct')) != float(tie_threshold_pct):
            return False
        return True
    except Exception:
        return False


def _load_report_any(path: Path) -> Optional[Dict]:
    try:
        import json
        path = Path(path)
        if not path.exists():
            return None
        if path.suffix.lower() == '.json':
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        if path.suffix.lower() == '.ndjson':
            # Expect a leading meta line (type=meta) but keep best-effort fallback.
            meta = None
            overall = None
            os_entries: Dict[str, Dict] = {}
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    t = obj.get('type')
                    if t == 'meta':
                        meta = obj.get('meta')
                    elif t == 'overall':
                        overall = {
                            'summary_a': obj.get('summary_a'),
                            'summary_b': obj.get('summary_b'),
                            'relative': obj.get('relative'),
                        }
                        host_a = obj.get('host_a')
                        host_b = obj.get('host_b')
                    elif t == 'os':
                        os_name = obj.get('os')
                        if not os_name:
                            continue
                        os_entries[os_name] = {
                            'os': os_name,
                            'summary_a': obj.get('summary_a'),
                            'summary_b': obj.get('summary_b'),
                            'relative': obj.get('relative'),
                            'by_format': [],
                        }
                    elif t == 'os_format':
                        os_name = obj.get('os')
                        if not os_name:
                            continue
                        if os_name not in os_entries:
                            os_entries[os_name] = {
                                'os': os_name,
                                'summary_a': None,
                                'summary_b': None,
                                'relative': None,
                                'by_format': [],
                            }
                        os_entries[os_name]['by_format'].append({
                            'format': obj.get('format'),
                            'summary_a': obj.get('summary_a'),
                            'summary_b': obj.get('summary_b'),
                            'relative': obj.get('relative'),
                        })

            if overall is None:
                return None
            report = {
                'host_a': host_a,
                'host_b': host_b,
                'tie_threshold_pct': None,
                'overall': overall,
                'by_os': [os_entries[k] for k in sorted(os_entries.keys())],
            }
            if meta:
                report['meta'] = meta
                report['tie_threshold_pct'] = (meta.get('args') or {}).get('tie_threshold_pct')
            return report
        return None
    except Exception:
        return None


def _export_report(report: Dict, json_out: Path, ndjson: bool = False) -> None:
    import json
    json_out = Path(json_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    if ndjson:
        with open(json_out, 'w', encoding='utf-8') as f:
            # Write meta first (enables caching and provenance)
            if report.get('meta'):
                f.write(json.dumps({'type': 'meta', 'meta': report['meta']}) + '\n')
            # Write overall
            f.write(json.dumps({'type': 'overall', **report['overall'], 'host_a': report['host_a'], 'host_b': report['host_b']}) + '\n')
            for os_entry in report.get('by_os', []):
                base = {'type': 'os', 'os': os_entry.get('os'), 'host_a': report['host_a'], 'host_b': report['host_b']}
                f.write(json.dumps({**base, 'summary_a': os_entry.get('summary_a'), 'summary_b': os_entry.get('summary_b'), 'relative': os_entry.get('relative')}) + '\n')
                for fmt_entry in os_entry.get('by_format', []) or []:
                    base_fmt = {'type': 'os_format', 'os': os_entry.get('os'), 'format': fmt_entry.get('format'), 'host_a': report['host_a'], 'host_b': report['host_b']}
                    f.write(json.dumps({**base_fmt, 'summary_a': fmt_entry.get('summary_a'), 'summary_b': fmt_entry.get('summary_b'), 'relative': fmt_entry.get('relative')}) + '\n')
    else:
        with open(json_out, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)


def _negate_relative(relative: Optional[Dict]) -> Optional[Dict]:
    if relative is None:
        return None
    out = dict(relative)
    if out.get('overall_mean_pct') is not None:
        out['overall_mean_pct'] = -out['overall_mean_pct']
    libs_pct = out.get('libs_pct') or {}
    out['libs_pct'] = {k: (-v if v is not None else None) for k, v in libs_pct.items()}
    return out


def _reorient_report(report: Dict, host_a: str, host_b: str) -> Dict:
    # Ensure report is oriented to the requested host order (host_a, host_b).
    if report.get('host_a') == host_a and report.get('host_b') == host_b:
        return report
    if report.get('host_a') == host_b and report.get('host_b') == host_a:
        swapped = dict(report)
        swapped['host_a'] = host_a
        swapped['host_b'] = host_b
        overall = dict(swapped.get('overall') or {})
        overall['summary_a'], overall['summary_b'] = overall.get('summary_b'), overall.get('summary_a')
        overall['relative'] = _negate_relative(overall.get('relative'))
        swapped['overall'] = overall
        by_os_new = []
        for os_entry in swapped.get('by_os', []) or []:
            e = dict(os_entry)
            e['summary_a'], e['summary_b'] = e.get('summary_b'), e.get('summary_a')
            e['relative'] = _negate_relative(e.get('relative'))
            bf = []
            for fmt_entry in e.get('by_format', []) or []:
                fe = dict(fmt_entry)
                fe['summary_a'], fe['summary_b'] = fe.get('summary_b'), fe.get('summary_a')
                fe['relative'] = _negate_relative(fe.get('relative'))
                bf.append(fe)
            e['by_format'] = bf
            by_os_new.append(e)
        swapped['by_os'] = by_os_new
        return swapped
    return report


def filter_rows_by_hosts(rows: List[Dict[str, str]], hosts: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """Partition rows by hostname into separate buckets.
    
    Args:
        rows: All benchmark result rows
        hosts: List of hostnames to filter for
        
    Returns:
        Dictionary mapping hostname -> list of rows for that host
        
    Example:
        >>> rows = [{'hostname': 'A', 'data': '1'}, {'hostname': 'B', 'data': '2'}]
        >>> filter_rows_by_hosts(rows, ['A', 'B'])
        {'A': [{'hostname': 'A', 'data': '1'}], 'B': [{'hostname': 'B', 'data': '2'}]}
    """
    buckets: Dict[str, List[Dict[str, str]]] = {h: [] for h in hosts}
    for r in rows:
        hn = r.get('hostname')
        if hn is None:
            continue
        for h in hosts:
            if hn == h:
                buckets[h].append(r)
    return buckets


def summarize_host(rows: List[Dict[str, str]], lib_ops: Dict[str, List[str]]) -> Dict:
    """Compute comprehensive performance summary statistics for a host.
    
    Analyzes all benchmark runs for a host and produces:
    - Hardware info (CPU brand, core counts, memory)
    - Per-library statistics (mean, median, best, percentiles, stdev, sample size)
    - Overall aggregate statistics across all libraries
    
    Args:
        rows: All benchmark rows for the host
        lib_ops: Dictionary mapping library names to their timing column names
                 (e.g., {'pandas': ['filter_group_pandas_seconds', ...]})
        
    Returns:
        Dictionary containing:
            - rows: Number of benchmark runs
            - cpu_brand: Most common CPU brand string
            - cpu_logical_mean: Average logical core count
            - cpu_physical_mean: Average physical core count
            - mem_total_mean: Average total memory (GB)
            - mem_avail_mean: Average available memory (GB)
            - libs: Per-library stats dict with keys: mean, median, best, p10, p25, p75, p90, stdev, n
            - overall_mean: Mean across all operations and libraries
            - overall_median: Median across all operations
            - overall_best: Best (minimum) timing across all runs
            - overall_p10/p25/p75/p90: Percentile values
            - overall_stdev: Standard deviation
            - overall_cv: Coefficient of variation (stdev/mean * 100)
            
    Note:
        For each row, computes mean timing across all 4 operations per library,
        then aggregates these per-row means to get robust statistics.
    """
    # Representative host attributes
    cpu_brand = None
    cpu_logical = None
    cpu_physical = None
    if rows:
        # Pick the most frequent cpu_brand if available
        brands = {}
        for r in rows:
            b = r.get('cpu_brand')
            if b:
                brands[b] = brands.get(b, 0) + 1
        if brands:
            cpu_brand = sorted(brands.items(), key=lambda x: x[1], reverse=True)[0][0]
        # Average core counts across rows
        try:
            vals = [fval(r.get('cpu_count_logical')) for r in rows]
            vals = [v for v in vals if v is not None]
            cpu_logical = mean(vals) if vals else None
        except Exception:
            cpu_logical = None
        try:
            vals = [fval(r.get('cpu_count_physical')) for r in rows]
            vals = [v for v in vals if v is not None]
            cpu_physical = mean(vals) if vals else None
        except Exception:
            cpu_physical = None

    mem_total = [fval(r.get('memory_total_gb')) for r in rows]
    mem_total = [v for v in mem_total if v is not None]
    mem_avail = [fval(r.get('memory_available_gb')) for r in rows]
    mem_avail = [v for v in mem_avail if v is not None]

    lib_means: Dict[str, Dict[str, float]] = {}
    for lib, cols in lib_ops.items():
        per_row_means: List[float] = []
        per_row_bests: List[float] = []
        for r in rows:
            vals = [fval(r.get(c)) for c in cols]
            vals = [v for v in vals if v is not None]
            if vals:
                per_row_means.append(mean(vals))
                per_row_bests.append(min(vals))
        if per_row_means:
            lib_means[lib] = {
                'mean': mean(per_row_means),
                'median': median(per_row_means),
                'best': min(per_row_bests),
                'p10': float(np.percentile(per_row_means, 10)),
                'p25': float(np.percentile(per_row_means, 25)),
                'p75': float(np.percentile(per_row_means, 75)),
                'p90': float(np.percentile(per_row_means, 90)),
                'stdev': stdev(per_row_means) if len(per_row_means) > 1 else 0.0,
                'n': len(per_row_means),
            }

    overall_per_row: List[float] = []
    overall_best_per_row: List[float] = []
    for r in rows:
        vals: List[Optional[float]] = []
        for cols in lib_ops.values():
            vals.extend([fval(r.get(c)) for c in cols])
        flat = [v for v in vals if v is not None]
        if flat:
            overall_per_row.append(mean(flat))
            overall_best_per_row.append(min(flat))

    overall_stats = {}
    if overall_per_row:
        overall_stats = {
            'overall_mean': mean(overall_per_row),
            'overall_median': median(overall_per_row),
            'overall_best': min(overall_best_per_row) if overall_best_per_row else None,
            'overall_p10': float(np.percentile(overall_per_row, 10)),
            'overall_p25': float(np.percentile(overall_per_row, 25)),
            'overall_p75': float(np.percentile(overall_per_row, 75)),
            'overall_p90': float(np.percentile(overall_per_row, 90)),
            'overall_stdev': stdev(overall_per_row) if len(overall_per_row) > 1 else 0.0,
            'overall_cv': (stdev(overall_per_row) / mean(overall_per_row) * 100) if len(overall_per_row) > 1 and mean(overall_per_row) > 0 else 0.0,
        }

    return {
        'rows': len(rows),
        'cpu_brand': cpu_brand,
        'cpu_logical_mean': cpu_logical,
        'cpu_physical_mean': cpu_physical,
        'mem_total_mean': mean(mem_total) if mem_total else None,
        'mem_avail_mean': mean(mem_avail) if mem_avail else None,
        'libs': lib_means,
        **overall_stats,
    }


def relative_pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Calculate percentage difference from a to b.
    
    Formula: (a - b) / a * 100
    
    Args:
        a: Baseline value (typically host A's timing)
        b: Comparison value (typically host B's timing)
        
    Returns:
        Percentage difference where:
        - Positive value means b is faster (duration decreased)
        - Negative value means b is slower (duration increased)
        - None if either value is None or a is zero
        
    Example:
        >>> relative_pct(10.0, 8.0)  # b is 20% faster
        20.0
        >>> relative_pct(8.0, 10.0)  # b is 25% slower
        -25.0
    """
    # Percent change from a -> b: (a-b)/a*100; negative means b is faster if values are durations
    if a is None or b is None or a == 0:
        return None
    return (a - b) / a * 100.0


def compare_hosts(csv_path: Path, host_a: str, host_b: str, remove_outliers: bool = False) -> Dict:
    """Compare benchmark performance between two hosts with optional outlier removal.
    
    Core comparison function that loads data, optionally removes outliers, and produces
    comprehensive host-to-host comparison including per-library and overall metrics.
    
    Args:
        csv_path: Path to benchmark_results.csv file
        host_a: First hostname (baseline)
        host_b: Second hostname (comparison)
        remove_outliers: If True, applies IQR-based outlier removal to both hosts.
                        Default False for backward compatibility, but tool default is True.
        
    Returns:
        Dictionary containing:
            - host_a, host_b: Hostnames
            - summary_a, summary_b: Full summary statistics from summarize_host()
            - relative: Percentage differences between hosts
                - overall_mean_pct: Overall percentage difference
                - libs_pct: Dict of per-library percentage differences
            - rows_all: Combined filtered rows (after outlier removal if enabled)
            
    Note:
        When remove_outliers=True, uses IQR method with 1.5x multiplier independently
        per library. Entire rows are removed if any library shows outlier performance.
        Typical removal rate: 1-2% of data.
        
    Example:
        >>> result = compare_hosts(Path('data/results.csv'), 'HostA', 'HostB', remove_outliers=True)
        >>> print(result['relative']['overall_mean_pct'])
        35.2  # HostB is 35.2% faster
    """
    rows = load_rows(csv_path)
    buckets = filter_rows_by_hosts(rows, [host_a, host_b])
    
    # Remove outliers if requested
    outliers_removed = {}
    if remove_outliers:
        for host in [host_a, host_b]:
            original_count = len(buckets.get(host, []))
            filtered, removed = remove_outliers_iqr(buckets.get(host, []), LIB_OPS)
            buckets[host] = filtered
            outliers_removed[host] = {'original': original_count, 'removed': removed, 'remaining': len(filtered)}

    # Initial summaries are recomputed later with filters; keep base here
    sum_a = summarize_host(buckets.get(host_a, []), LIB_OPS)
    sum_b = summarize_host(buckets.get(host_b, []), LIB_OPS)

    rel = {
        'overall_mean_pct': relative_pct(sum_a.get('overall_mean'), sum_b.get('overall_mean')),
        'libs_pct': {},
    }
    for lib in LIB_OPS.keys():
        a_mean = sum_a.get('libs', {}).get(lib, {}).get('mean')
        b_mean = sum_b.get('libs', {}).get(lib, {}).get('mean')
        rel['libs_pct'][lib] = relative_pct(a_mean, b_mean)

    # Reassemble filtered rows for by-OS analysis
    filtered_rows = buckets.get(host_a, []) + buckets.get(host_b, [])
    
    result = {
        'host_a': host_a,
        'host_b': host_b,
        'summary_a': sum_a,
        'summary_b': sum_b,
        'relative': rel,
        'rows_all': filtered_rows,  # Use filtered rows if outliers were removed
    }
    
    if remove_outliers and outliers_removed:
        result['outliers_removed'] = outliers_removed
    
    return result


def fmt_float(x: Optional[float], digits: int = 3) -> str:
    if x is None:
        return 'N/A'
    return f"{x:.{digits}f}"


def _os_values_for_hosts(rows: List[Dict[str, str]], host_a: str, host_b: str) -> List[str]:
    os_a = set()
    os_b = set()
    for r in rows:
        hn = r.get('hostname')
        sys = r.get('system')
        if not hn or not sys:
            continue
        if hn == host_a:
            os_a.add(sys)
        if hn == host_b:
            os_b.add(sys)
    return sorted(list(os_a.intersection(os_b)))


def _filter_rows_for_host_os(rows: List[Dict[str, str]], host: str, system: str) -> List[Dict[str, str]]:
    out = []
    for r in rows:
        hn = r.get('hostname')
        sys = r.get('system')
        if not hn or not sys:
            continue
        if sys != system:
            continue
        if hn == host:
            out.append(r)
    return out

def _formats_for_hosts_os(rows: List[Dict[str, str]], host_a: str, host_b: str, system: str) -> List[str]:
    fa = set()
    fb = set()
    for r in rows:
        hn = r.get('hostname')
        sys = r.get('system')
        fmt = r.get('dataset_format')
        if not hn or not sys or not fmt:
            continue
        if sys != system:
            continue
        if hn == host_a:
            fa.add(fmt)
        elif hn == host_b:
            fb.add(fmt)
    return sorted(list(fa.intersection(fb)))

def _filter_rows_for_host_os_fmt(rows: List[Dict[str, str]], host: str, system: str, fmt: str) -> List[Dict[str, str]]:
    out = []
    for r in rows:
        if r.get('hostname') != host:
            continue
        if r.get('system') != system:
            continue
        if r.get('dataset_format') != fmt:
            continue
        out.append(r)
    return out


def _filter_rows_by_formats(rows: List[Dict[str, str]], formats: Optional[List[str]]) -> List[Dict[str, str]]:
    if not formats:
        return rows
    allow = set([f.lower() for f in formats])
    out = []
    for r in rows:
        fmt = (r.get('dataset_format') or '').lower()
        if fmt in allow:
            out.append(r)
    return out


def _print_console(report: Dict, tie_threshold_pct: float = 5.0, quiet: bool = False) -> None:
    a = report.get('overall', {}).get('summary_a') or {}
    b = report.get('overall', {}).get('summary_b') or {}
    rel = report.get('overall', {}).get('relative') or {}

    meta = report.get('meta')
    if meta:
        via = meta.get('generated_via')
        src = meta.get('source_report')
        if via == 'cache':
            if src:
                print(f"Source: cache ({src})")
            else:
                print("Source: cache")
        elif via == 'compute':
            csv_info = meta.get('csv') or {}
            print(f"Source: computed from CSV ({csv_info.get('path')})")

    # Headline verdict (one-liner at the very top)
    ov = rel.get('overall_mean_pct')
    if ov is None:
        print("Winner: N/A")
    else:
        if abs(ov) < tie_threshold_pct:
            print(f"Winner: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif ov > 0:
            print(f"Winner: {report['host_b']} (~{ov:.2f}% faster overall)")
        else:
            print(f"Winner: {report['host_a']} (~{abs(ov):.2f}% faster overall)")
    
    # Report outlier removal if applicable
    outliers_info = report.get('outliers_removed')
    if outliers_info:
        print("\n== Outlier Removal (IQR Method) ==")
        for host, stats in outliers_info.items():
            print(f"{host}: {stats['removed']} outliers removed ({stats['remaining']}/{stats['original']} rows retained)")
        print("")

    print("== Summary ==")
    if ov is None:
        print("- Overall: N/A")
    else:
        if abs(ov) < tie_threshold_pct:
            print(f"- Overall: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif ov > 0:
            print(f"- Overall: {report['host_b']} faster by {ov:.2f}%")
        else:
            print(f"- Overall: {report['host_a']} faster by {abs(ov):.2f}%")

    libs_pct = (rel.get('libs_pct') or {})
    for lib in [l for l in ['pandas', 'polars', 'duckdb', 'fireducks'] if l in libs_pct]:
        p = libs_pct.get(lib)
        if p is None:
            print(f"- {lib}: N/A")
            continue
        if abs(p) < tie_threshold_pct:
            print(f"- {lib}: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif p > 0:
            print(f"- {lib}: {report['host_b']} faster by {p:.2f}%")
        else:
            print(f"- {lib}: {report['host_a']} faster by {abs(p):.2f}%")

    if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
        mem_a = a['mem_avail_mean']
        mem_b = b['mem_avail_mean']
        if abs(mem_a - mem_b) < 1e-9:
            print(f"- Memory: Similar available RAM (~{mem_a:.2f} GB)")
        elif mem_b > mem_a:
            print(f"- Memory: {report['host_b']} higher available RAM ({mem_b:.2f} GB vs {mem_a:.2f} GB)")
        else:
            print(f"- Memory: {report['host_a']} higher available RAM ({mem_a:.2f} GB vs {mem_b:.2f} GB)")
    print("")

    if not quiet:
        print("== Host A ==")
        print(f"Rows: {a.get('rows')}")
        print(f"CPU: {a.get('cpu_brand') or 'N/A'} | logical ~ {fmt_float(a.get('cpu_logical_mean'))} | physical ~ {fmt_float(a.get('cpu_physical_mean'))}")
        print(f"Mem Total Mean (GB): {fmt_float(a.get('mem_total_mean'))}")
        print(f"Mem Avail Mean (GB): {fmt_float(a.get('mem_avail_mean'))}")
        for lib, stats in (a.get('libs') or {}).items():
            print(f"{lib} mean: {fmt_float(stats.get('mean'))} | median: {fmt_float(stats.get('median'))} | best: {fmt_float(stats.get('best'))} | n: {stats.get('n')}")
        print(f"Overall mean: {fmt_float(a.get('overall_mean'))} | median: {fmt_float(a.get('overall_median'))} | best: {fmt_float(a.get('overall_best'))}")

        print("\n== Host B ==")
        print(f"Rows: {b.get('rows')}")
        print(f"CPU: {b.get('cpu_brand') or 'N/A'} | logical ~ {fmt_float(b.get('cpu_logical_mean'))} | physical ~ {fmt_float(b.get('cpu_physical_mean'))}")
        print(f"Mem Total Mean (GB): {fmt_float(b.get('mem_total_mean'))}")
        print(f"Mem Avail Mean (GB): {fmt_float(b.get('mem_avail_mean'))}")
        for lib, stats in (b.get('libs') or {}).items():
            print(f"{lib} mean: {fmt_float(stats.get('mean'))} | median: {fmt_float(stats.get('median'))} | best: {fmt_float(stats.get('best'))} | n: {stats.get('n')}")
        print(f"Overall mean: {fmt_float(b.get('overall_mean'))} | median: {fmt_float(b.get('overall_median'))} | best: {fmt_float(b.get('overall_best'))}")

        print("\n== Relative (who is faster) ==")
        # Compute additional metrics
        med_a = a.get('overall_median')
        med_b = b.get('overall_median')
        med_pct = relative_pct(med_a, med_b)
        best_a = a.get('overall_best')
        best_b = b.get('overall_best')
        best_pct = relative_pct(best_a, best_b)
        
        if ov is None:
            print("Overall (mean): N/A")
        else:
            if abs(ov) < tie_threshold_pct:
                print(f"Overall (mean): Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif ov > 0:
                print(f"Overall (mean): {report['host_b']} faster by {ov:.2f}%")
            else:
                print(f"Overall (mean): {report['host_a']} faster by {abs(ov):.2f}%")
        
        if med_pct is None:
            print("Overall (median): N/A")
        else:
            if abs(med_pct) < tie_threshold_pct:
                print(f"Overall (median): Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif med_pct > 0:
                print(f"Overall (median): {report['host_b']} faster by {med_pct:.2f}%")
            else:
                print(f"Overall (median): {report['host_a']} faster by {abs(med_pct):.2f}%")
        
        if best_pct is None:
            print("Overall (best): N/A")
        else:
            if abs(best_pct) < tie_threshold_pct:
                print(f"Overall (best): Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif best_pct > 0:
                print(f"Overall (best): {report['host_b']} faster by {best_pct:.2f}%")
            else:
                print(f"Overall (best): {report['host_a']} faster by {abs(best_pct):.2f}%")
        
        for lib in libs_pct.keys():
            pct = libs_pct.get(lib)
            if pct is None:
                print(f"{lib}: N/A")
            elif abs(pct) < tie_threshold_pct:
                print(f"{lib}: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif pct > 0:
                print(f"{lib}: {report['host_b']} faster by {pct:.2f}%")
            else:
                print(f"{lib}: {report['host_a']} faster by {abs(pct):.2f}%")

    print("\n== Verdict ==")
    if ov is not None:
        if abs(ov) < tie_threshold_pct:
            print(f"Winner: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif ov > 0:
            print(f"Winner: {report['host_b']} (~{ov:.2f}% faster)")
        else:
            print(f"Winner: {report['host_a']} (~{abs(ov):.2f}% faster)")

    print("Bottom Line")
    if ov is not None:
        faster = "faster" if ov > 0 else "slower"
        print(f"- {report['host_b']} vs {report['host_a']} (overall): about {abs(ov):.2f}% {faster} on average across all ops.")
    lib_lines = []
    for lib in [l for l in ['pandas', 'polars', 'duckdb', 'fireducks'] if l in libs_pct]:
        p = libs_pct.get(lib)
        if p is not None:
            faster = "faster" if p > 0 else "slower"
            lib_lines.append(f"{lib} ~{abs(p):.2f}% {faster}")
    if lib_lines:
        print(f"- By library: {'; '.join(lib_lines)}.")
    if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
        mem_a = a['mem_avail_mean']
        mem_b = b['mem_avail_mean']
        if abs(mem_a - mem_b) < 1e-9:
            print(f"- Memory: Both show similar average available RAM (~{mem_a:.2f} GB).")
        elif mem_b > mem_a:
            print(f"- Memory: {report['host_b']} shows higher average available RAM ({mem_b:.2f} GB vs {mem_a:.2f} GB).")
        else:
            print(f"- Memory: {report['host_a']} shows higher average available RAM ({mem_a:.2f} GB vs {mem_b:.2f} GB).")

    if not quiet:
        print("\nMethodology")
        print("- Compared mean operation times per host across four ops: filter_group, statistics, complex_join, timeseries.")
        print("- Aggregated per library (pandas, polars, duckdb, fireducks where available), then averaged across ops.")
        print("- Report includes per-OS and per-format (e.g., CSV, Parquet) breakdowns when both hosts have data.")

        print("\nKey Numbers")
        print(f"- {report['host_a']} overall: mean {fmt_float(a.get('overall_mean'))} | median {fmt_float(a.get('overall_median'))} | best {fmt_float(a.get('overall_best'))} s")
        print(f"- {report['host_b']} overall: mean {fmt_float(b.get('overall_mean'))} | median {fmt_float(b.get('overall_median'))} | best {fmt_float(b.get('overall_best'))} s")
        if ov is not None:
            if abs(ov) < tie_threshold_pct:
                print(f"- Overall (mean): Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif ov > 0:
                print(f"- Overall (mean): {report['host_b']} faster by {ov:.2f}%")
            else:
                print(f"- Overall (mean): {report['host_a']} faster by {abs(ov):.2f}%")
        if med_pct is not None:
            if abs(med_pct) < tie_threshold_pct:
                print(f"- Overall (median): Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif med_pct > 0:
                print(f"- Overall (median): {report['host_b']} faster by {med_pct:.2f}%")
            else:
                print(f"- Overall (median): {report['host_a']} faster by {abs(med_pct):.2f}%")
        if best_pct is not None:
            if abs(best_pct) < tie_threshold_pct:
                print(f"- Overall (best): Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif best_pct > 0:
                print(f"- Overall (best): {report['host_b']} faster by {best_pct:.2f}%")
            else:
                print(f"- Overall (best): {report['host_a']} faster by {abs(best_pct):.2f}%")
        for lib in [l for l in ['pandas', 'polars', 'duckdb', 'fireducks'] if l in libs_pct]:
            p = libs_pct.get(lib)
            if p is None:
                print(f"- {lib}: N/A")
            elif abs(p) < tie_threshold_pct:
                print(f"- {lib}: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif p > 0:
                print(f"- {lib}: {report['host_b']} faster by {p:.2f}%")
            else:
                print(f"- {lib}: {report['host_a']} faster by {abs(p):.2f}%")
        if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
            print(f"- Average memory available: {report['host_a']} ~ {a['mem_avail_mean']:.2f} GB; {report['host_b']} ~ {b['mem_avail_mean']:.2f} GB")

        # Advanced fairness metrics
        print("\n== Advanced Analysis ==")
        
        # Stability / Consistency scores
        print("\nStability (lower CV = more consistent):")
        cv_a = a.get('overall_cv', 0)
        cv_b = b.get('overall_cv', 0)
        print(f"- {report['host_a']}: CV = {cv_a:.1f}% (stdev: {fmt_float(a.get('overall_stdev'))})")
        print(f"- {report['host_b']}: CV = {cv_b:.1f}% (stdev: {fmt_float(b.get('overall_stdev'))})")
        if cv_a < cv_b * 0.8:
            print(f"  → {report['host_a']} is significantly more stable")
        elif cv_b < cv_a * 0.8:
            print(f"  → {report['host_b']} is significantly more stable")
        else:
            print("  → Both hosts show similar stability")
        
        # Percentile comparisons (robust to outliers)
        print("\nPercentile Comparison (more robust than mean):")
        for pct in [10, 25, 75, 90]:
            p_a = a.get(f'overall_p{pct}')
            p_b = b.get(f'overall_p{pct}')
            if p_a is not None and p_b is not None:
                diff_pct = relative_pct(p_a, p_b)
                if diff_pct is not None:
                    if abs(diff_pct) < tie_threshold_pct:
                        winner = "Tie"
                    elif diff_pct > 0:
                        winner = f"{report['host_b']} {abs(diff_pct):.1f}% faster"
                    else:
                        winner = f"{report['host_a']} {abs(diff_pct):.1f}% faster"
                    print(f"- P{pct}: {fmt_float(p_a)} vs {fmt_float(p_b)} → {winner}")
        
        # OS-weighted average (fair when row counts differ)
        by_os = report.get('by_os') or []
        if len(by_os) > 1:
            print("\nOS-Weighted Overall (equal weight per OS, fairer than raw mean):")
            os_means_a = []
            os_means_b = []
            for os_entry in by_os:
                sa = os_entry.get('summary_a') or {}
                sb = os_entry.get('summary_b') or {}
                ma = sa.get('overall_mean')
                mb = sb.get('overall_mean')
                if ma is not None and mb is not None:
                    os_means_a.append(ma)
                    os_means_b.append(mb)
            if os_means_a and os_means_b:
                os_weighted_a = mean(os_means_a)
                os_weighted_b = mean(os_means_b)
                os_weighted_pct = relative_pct(os_weighted_a, os_weighted_b)
                print(f"- {report['host_a']}: {os_weighted_a:.3f}s")
                print(f"- {report['host_b']}: {os_weighted_b:.3f}s")
                if os_weighted_pct is not None:
                    if abs(os_weighted_pct) < tie_threshold_pct:
                        print(f"  → Tie (within {tie_threshold_pct:.1f}%)")
                    elif os_weighted_pct > 0:
                        print(f"  → {report['host_b']} {abs(os_weighted_pct):.1f}% faster (OS-weighted)")
                    else:
                        print(f"  → {report['host_a']} {abs(os_weighted_pct):.1f}% faster (OS-weighted)")
        
        # Memory efficiency (performance per GB available)
        mem_a = a.get('mem_avail_mean')
        mem_b = b.get('mem_avail_mean')
        if mem_a and mem_b and a.get('overall_median') and b.get('overall_median'):
            mem_eff_a = a['overall_median'] / mem_a
            mem_eff_b = b['overall_median'] / mem_b
            print(f"\nMemory Efficiency (time/GB, lower = better use of RAM):")
            print(f"- {report['host_a']}: {mem_eff_a:.4f} s/GB")
            print(f"- {report['host_b']}: {mem_eff_b:.4f} s/GB")
            if mem_eff_a < mem_eff_b * 0.9:
                print(f"  → {report['host_a']} uses RAM more efficiently")
            elif mem_eff_b < mem_eff_a * 0.9:
                print(f"  → {report['host_b']} uses RAM more efficiently")
        
        # Library specialization (which host is best at which library)
        print("\nLibrary Specialization:")
        for lib in ['pandas', 'polars', 'duckdb', 'fireducks']:
            lib_a = a.get('libs', {}).get(lib)
            lib_b = b.get('libs', {}).get(lib)
            if lib_a and lib_b:
                med_a = lib_a.get('median')
                med_b = lib_b.get('median')
                if med_a and med_b:
                    diff = relative_pct(med_a, med_b)
                    if diff is not None:
                        if abs(diff) < tie_threshold_pct:
                            verdict = "Tie"
                        elif diff > 0:
                            verdict = f"{report['host_b']} better ({abs(diff):.1f}%)"
                        else:
                            verdict = f"{report['host_a']} better ({abs(diff):.1f}%)"
                        print(f"- {lib}: {verdict}")
        
        print("\nRecommendation:")
        # Count wins across different metrics
        wins_a = 0
        wins_b = 0
        
        # Mean
        if ov is not None and abs(ov) >= tie_threshold_pct:
            if ov < 0:
                wins_a += 1
            else:
                wins_b += 1
        
        # Median
        if med_pct is not None and abs(med_pct) >= tie_threshold_pct:
            if med_pct < 0:
                wins_a += 1
            else:
                wins_b += 1
        
        # Stability
        if cv_a < cv_b * 0.8:
            wins_a += 1
        elif cv_b < cv_a * 0.8:
            wins_b += 1
        
        if wins_a > wins_b:
            print(f"→ {report['host_a']} is the better overall choice for data analysis workloads")
        elif wins_b > wins_a:
            print(f"→ {report['host_b']} is the better overall choice for data analysis workloads")
        else:
            print("→ Both hosts are competitive; choose based on budget, availability, and specific library needs")

        print("\nInterpretation")
        a_log = a.get('cpu_logical_mean')
        a_phy = a.get('cpu_physical_mean')
        b_log = b.get('cpu_logical_mean')
        b_phy = b.get('cpu_physical_mean')
        print(f"- {report['host_b']}'s {b.get('cpu_brand') or 'CPU'} ({fmt_float(b_log,0)} logical / {fmt_float(b_phy,0)} physical) vs {report['host_a']}'s {a.get('cpu_brand') or 'CPU'} ({fmt_float(a_log,0)} / {fmt_float(a_phy,0)}).")
        print("- Advantages hold across CSV and Parquet; Parquet tends to be faster overall, but relative differences persist.")
        if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
            print("- Higher available RAM on the faster host adds headroom for larger datasets and complex joins.")

        by_os = report.get('by_os') or []
        if by_os:
            print("\n== By OS ==")
            for os_entry in by_os:
                os_name = os_entry.get('os')
                if not os_name:
                    continue
                print(f"\n-- {os_name} --")
                rel_os = os_entry.get('relative') or {}
                ov_os = rel_os.get('overall_mean_pct')
                if ov_os is None:
                    print("Insufficient data to compare.")
                    continue
                if abs(ov_os) < tie_threshold_pct:
                    print(f"Winner: Tie (within {tie_threshold_pct:.1f}% threshold)")
                elif ov_os > 0:
                    print(f"Winner: {report['host_b']} (~{ov_os:.2f}% faster)")
                else:
                    print(f"Winner: {report['host_a']} (~{abs(ov_os):.2f}% faster)")
                sa = os_entry.get('summary_a') or {}
                sb = os_entry.get('summary_b') or {}
                print(f"- Overall mean: {report['host_a']} {fmt_float(sa.get('overall_mean'))} s vs {report['host_b']} {fmt_float(sb.get('overall_mean'))} s")
                lib_lines = []
                libs_pct_os = (rel_os.get('libs_pct') or {})
                for lib in [l for l in ['pandas','polars','duckdb','fireducks'] if l in libs_pct_os]:
                    p = libs_pct_os.get(lib)
                    if p is None:
                        lib_lines.append(f"{lib} N/A")
                    elif abs(p) < tie_threshold_pct:
                        lib_lines.append(f"{lib} Tie")
                    elif p > 0:
                        lib_lines.append(f"{lib} {report['host_b']} faster by {p:.2f}%")
                    else:
                        lib_lines.append(f"{lib} {report['host_a']} faster by {abs(p):.2f}%")
                if lib_lines:
                    print("- Per-library:", '; '.join(lib_lines))

                for fmt_entry in os_entry.get('by_format', []) or []:
                    rel_fmt = fmt_entry.get('relative') or {}
                    ov_fmt = rel_fmt.get('overall_mean_pct')
                    if ov_fmt is None:
                        continue
                    fmt_name = fmt_entry.get('format')
                    title = (fmt_name.upper() if fmt_name else fmt_name)
                    if abs(ov_fmt) < tie_threshold_pct:
                        print(f"  * {title}: Tie (within {tie_threshold_pct:.1f}% threshold)")
                    elif ov_fmt > 0:
                        print(f"  * {title}: {report['host_b']} (~{ov_fmt:.2f}% faster)")
                    else:
                        print(f"  * {title}: {report['host_a']} (~{abs(ov_fmt):.2f}% faster)")
                    sfa = fmt_entry.get('summary_a') or {}
                    sfb = fmt_entry.get('summary_b') or {}
                    print(f"    - Overall mean: {report['host_a']} {fmt_float(sfa.get('overall_mean'))} s vs {report['host_b']} {fmt_float(sfb.get('overall_mean'))} s")
                    lib_lines_fmt = []
                    libs_pct_fmt = (rel_fmt.get('libs_pct') or {})
                    for lib in [l for l in ['pandas','polars','duckdb','fireducks'] if l in libs_pct_fmt]:
                        p = libs_pct_fmt.get(lib)
                        if p is None:
                            lib_lines_fmt.append(f"{lib} N/A")
                        elif abs(p) < tie_threshold_pct:
                            lib_lines_fmt.append(f"{lib} Tie")
                        elif p > 0:
                            lib_lines_fmt.append(f"{lib} {report['host_b']} faster by {p:.2f}%")
                        else:
                            lib_lines_fmt.append(f"{lib} {report['host_a']} faster by {abs(p):.2f}%")
                    if lib_lines_fmt:
                        print("    - Per-library:", '; '.join(lib_lines_fmt))


def print_report(
    result: Dict,
    tie_threshold_pct: float = 5.0,
    libs: Optional[List[str]] = None,
    formats: Optional[List[str]] = None,
    json_out: Optional[Path] = None,
    ndjson: bool = False,
    quiet: bool = False,
    meta: Optional[Dict] = None,
) -> Dict:
    """Generate comprehensive comparison report with console output and optional JSON/NDJSON export.
    
    Produces detailed comparison including:
    - Overall summary and verdict
    - Per-OS breakdown (Windows, Linux, WSL2)
    - Per-format breakdown (CSV, Parquet, JSON, NDJSON)
    - Per-library performance metrics
    - Advanced analysis (stability, percentiles, memory efficiency)
    - Recommendations based on multiple metrics
    
    Args:
        result: Dictionary from compare_hosts() containing summaries and rows
        tie_threshold_pct: Percentage threshold below which results are considered a tie (default: 5.0)
        libs: Optional list of libraries to include in analysis (default: all available)
        formats: Optional list of file formats to filter by (csv, parquet, json, ndjson)
        json_out: Optional path for JSON/NDJSON export
        ndjson: If True, export as NDJSON (line-delimited JSON) instead of single JSON document
        quiet: If True, print only summary and verdict (omit detailed sections)
        meta: Optional metadata dictionary for provenance tracking
        
    Returns:
        Complete report dictionary with structure:
            - host_a, host_b: Hostnames
            - tie_threshold_pct: Tie threshold used
            - overall: Dict with summary_a, summary_b, relative percentages
            - by_os: List of per-OS comparisons
            - meta: Optional provenance metadata
            
    Note:
        Console output includes:
        - One-line winner verdict at top
        - Summary section with key percentages
        - Detailed host information (if not quiet)
        - Advanced analysis with stability, percentiles, specialization
        - Per-OS and per-format breakdowns
        
    Example:
        >>> result = compare_hosts(csv_path, 'HostA', 'HostB', remove_outliers=True)
        >>> report = print_report(result, tie_threshold_pct=5.0, json_out=Path('report.json'))
        Winner: HostB (~35.2% faster overall)
    """
    # Apply library selection
    libs_selected = [l for l in (libs or LIB_OPS.keys()) if l in LIB_OPS]
    lib_ops = {l: LIB_OPS[l] for l in libs_selected}

    # Use summaries from result (which may have outliers removed), not recomputed from rows_all
    a = result.get('summary_a', {})
    b = result.get('summary_b', {})
    
    # Apply format filtering to overall rows for by-OS/by-format analysis
    rows_all = _filter_rows_by_formats(result.get('rows_all', []), formats)
    
    rel = {
        'overall_mean_pct': relative_pct(a.get('overall_mean'), b.get('overall_mean')),
        'overall_median_pct': relative_pct(a.get('overall_median'), b.get('overall_median')),
        'overall_best_pct': relative_pct(a.get('overall_best'), b.get('overall_best')),
        'libs_pct': {l: relative_pct(a.get('libs', {}).get(l, {}).get('mean'), b.get('libs', {}).get(l, {}).get('mean')) for l in lib_ops.keys()}
    }

    report: Dict = {
        'host_a': result['host_a'],
        'host_b': result['host_b'],
        'tie_threshold_pct': tie_threshold_pct,
        'overall': {
            'summary_a': a,
            'summary_b': b,
            'relative': rel,
        },
        'by_os': [],
    }
    if meta:
        report['meta'] = meta

    # Build per-OS/per-format sections (independent of quiet; quiet only affects console output)
    os_list = _os_values_for_hosts(rows_all, result['host_a'], result['host_b'])
    for os_name in os_list:
        rows_a_os = _filter_rows_for_host_os(rows_all, result['host_a'], os_name)
        rows_b_os = _filter_rows_for_host_os(rows_all, result['host_b'], os_name)
        sum_a_os = summarize_host(rows_a_os, lib_ops)
        sum_b_os = summarize_host(rows_b_os, lib_ops)
        rel_os = {
            'overall_mean_pct': relative_pct(sum_a_os.get('overall_mean'), sum_b_os.get('overall_mean')),
            'libs_pct': {l: relative_pct(sum_a_os.get('libs', {}).get(l, {}).get('mean'), sum_b_os.get('libs', {}).get(l, {}).get('mean')) for l in lib_ops.keys()}
        }

        os_entry = {
            'os': os_name,
            'summary_a': sum_a_os,
            'summary_b': sum_b_os,
            'relative': rel_os,
            'by_format': []
        }

        # Per-format breakdown within OS
        fmts = _formats_for_hosts_os(rows_all, result['host_a'], result['host_b'], os_name)
        if formats:
            allowed = {x.lower() for x in formats}
            fmts = [f for f in fmts if (f or '').lower() in allowed]
        for fmt in fmts:
            rows_a_os_fmt = _filter_rows_for_host_os_fmt(rows_all, result['host_a'], os_name, fmt)
            rows_b_os_fmt = _filter_rows_for_host_os_fmt(rows_all, result['host_b'], os_name, fmt)
            sum_a_os_fmt = summarize_host(rows_a_os_fmt, lib_ops)
            sum_b_os_fmt = summarize_host(rows_b_os_fmt, lib_ops)
            rel_os_fmt = {
                'overall_mean_pct': relative_pct(sum_a_os_fmt.get('overall_mean'), sum_b_os_fmt.get('overall_mean')),
                'libs_pct': {l: relative_pct(sum_a_os_fmt.get('libs', {}).get(l, {}).get('mean'), sum_b_os_fmt.get('libs', {}).get(l, {}).get('mean')) for l in lib_ops.keys()}
            }
            os_entry['by_format'].append({
                'format': fmt,
                'summary_a': sum_a_os_fmt,
                'summary_b': sum_b_os_fmt,
                'relative': rel_os_fmt,
            })

        report['by_os'].append(os_entry)

    # Console output
    _print_console(report, tie_threshold_pct=tie_threshold_pct, quiet=quiet)

    # Optional JSON/NDJSON export
    if json_out:
        try:
            _export_report(report, json_out, ndjson=ndjson)
            print(f"\nReport saved to {json_out} ({'NDJSON' if ndjson else 'JSON'})")
        except Exception as e:
            print(f"Failed to write report JSON: {e}")

    return report


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line interface for host-to-host benchmark comparison.
    
    Entry point for the compare_hosts.py script. Parses arguments, validates hostnames,
    manages caching, and orchestrates the comparison workflow.
    
    Command-line Arguments:
        --csv PATH: Path to benchmark_results.csv (default: data/benchmark_results.csv)
        --host HOSTNAME: Hostname to compare (use twice for two hosts)
        --tie-threshold-pct FLOAT: Percentage threshold for ties (default: 5.0)
        --formats FORMAT [...]: Restrict to specific formats (csv, parquet, json, ndjson)
        --libs LIBS: Comma-separated libraries to include (default: all)
        --json-out PATH: Output path for JSON/NDJSON report
        --ndjson: Export as NDJSON (line-delimited) instead of single JSON
        --quiet: Print only summary and verdict
        --out-dir PATH: Default directory for inferred output files
        --no-export: Skip JSON/NDJSON export (console only)
        --force: Force recomputation, ignore cached reports
        --keep-outliers: Disable automatic outlier removal (default: remove outliers)
    
    Args:
        argv: Optional list of command-line arguments (default: sys.argv)
        
    Returns:
        Exit code: 0 for success, 2 for errors (missing hostnames, invalid arguments)
        
    Caching Behavior:
        The tool automatically caches results to avoid recomputation:
        - Cache key: hosts (unordered), formats, libs, tie threshold, dataset signature
        - Checks both compare_A_vs_B and compare_B_vs_A filenames
        - Checks both .json and .ndjson extensions
        - Use --force to bypass cache
        
    Outlier Removal (Default Behavior):
        By default, statistical outliers are automatically removed using the IQR method.
        This provides more accurate comparisons by eliminating:
        - Thermal throttling events
        - Background process interference
        - Early buggy script versions
        Typical removal rate: 1-2% of data
        Use --keep-outliers to disable this feature.
        
    Example Usage:
        # Basic comparison with default outlier removal
        python compare_hosts.py --csv data/results.csv --host HostA --host HostB
        
        # Keep outliers and export to custom location
        python compare_hosts.py --csv data/results.csv --host HostA --host HostB \\
            --keep-outliers --json-out custom/report.json
        
        # Filter by libraries and formats, quiet output
        python compare_hosts.py --csv data/results.csv --host HostA --host HostB \\
            --libs pandas,polars --formats csv,parquet --quiet
    """
    parser = argparse.ArgumentParser(description="Compare two hosts from benchmark results CSV")
    parser.add_argument('--csv', type=Path, default=Path('data/benchmark_results.csv'), help='Path to results CSV')
    parser.add_argument('--host', action='append', required=True, help='Hostname to include (use twice). Supports wildcards if --wildcard is set.')
    # Wildcards removed: exact hostnames only
    parser.add_argument('--tie-threshold-pct', type=float, default=5.0, help='Threshold (percent) under which results are considered a tie')
    parser.add_argument('--formats', nargs='+', help='Restrict to these dataset formats (e.g., csv parquet)')
    parser.add_argument('--libs', type=str, help='Comma-separated libraries to include (default: all). Options: pandas,polars,duckdb,fireducks')
    parser.add_argument('--json-out', type=Path, help='Write report to this JSON/NDJSON file (optional; inferred from hosts if omitted)')
    parser.add_argument('--ndjson', action='store_true', help='Write report as NDJSON (one JSON per line)')
    parser.add_argument('--quiet', action='store_true', help='Print only Summary and Verdict for quick reading')
    parser.add_argument('--out-dir', type=Path, help='Directory for inferred output file when --json-out is omitted (default: data/results)')
    parser.add_argument('--no-export', action='store_true', help='Do not write JSON/NDJSON; print to console only')
    parser.add_argument('--force', action='store_true', help='Force recomputation (ignore any cached JSON/NDJSON report)')
    parser.add_argument('--keep-outliers', action='store_true', help='Keep statistical outliers (by default, outliers are removed using IQR method)')

    args = parser.parse_args(argv)
    if len(args.host) != 2:
        print('Please provide exactly two --host arguments')
        return 2

    host_a, host_b = args.host

    # Parse libs early so cache validation can include it.
    libs = None
    if args.libs:
        libs = [x.strip().lower() for x in args.libs.split(',') if x.strip()]

    # Load rows once
    rows = load_rows(args.csv)
    hostnames = sorted({r.get('hostname') for r in rows if r.get('hostname')})
    missing = [h for h in (host_a, host_b) if h not in hostnames]
    if missing:
        print(f"Error: hostname(s) not found: {', '.join(missing)}")
        close = []
        for h in missing:
            close.extend([m for m in difflib.get_close_matches(h, hostnames, n=5, cutoff=0.5)])
        if close:
            print("Did you mean:", ", ".join(sorted(set(close))))
        return 2

    # Handle export options
    if args.no_export:
        args.json_out = None
    else:
        # Infer default output path if not provided
        if args.json_out is None:
            ext = 'ndjson' if args.ndjson else 'json'
            default_dir = args.out_dir if args.out_dir else Path('data/results')
            args.json_out = default_dir / f"compare_{host_a}_vs_{host_b}.{ext}"

    # Cache lookup (use existing report when inputs/measurements haven't changed)
    # Cache is keyed by: hosts (unordered), formats, libs, tie-threshold, and a lightweight signature of the
    # effective dataset (rows + max timestamp per host after applying --formats).
    rows_effective = _filter_rows_by_formats(rows, args.formats)
    current_sig = {
        host_a: _dataset_signature_for_host(rows_effective, host_a),
        host_b: _dataset_signature_for_host(rows_effective, host_b),
    }

    if not args.force:
        # Candidate cache files:
        # 1) exact target if known
        # 2) inferred reverse-host name (json/ndjson)
        candidates: List[Path] = []
        if args.json_out:
            candidates.append(Path(args.json_out))
        # When output is inferred, also try reverse filename in same directory.
        if args.json_out and args.json_out.name.startswith('compare_') and '_vs_' in args.json_out.name:
            # Best-effort: also check both extensions in the same out dir.
            out_dir = Path(args.json_out).parent
            candidates.append(out_dir / f"compare_{host_b}_vs_{host_a}.json")
            candidates.append(out_dir / f"compare_{host_b}_vs_{host_a}.ndjson")
            candidates.append(out_dir / f"compare_{host_a}_vs_{host_b}.json")
            candidates.append(out_dir / f"compare_{host_a}_vs_{host_b}.ndjson")
        else:
            # Even without a target file, try out-dir default locations.
            out_dir = args.out_dir if args.out_dir else Path('data/results')
            candidates.append(out_dir / f"compare_{host_a}_vs_{host_b}.json")
            candidates.append(out_dir / f"compare_{host_b}_vs_{host_a}.json")
            candidates.append(out_dir / f"compare_{host_a}_vs_{host_b}.ndjson")
            candidates.append(out_dir / f"compare_{host_b}_vs_{host_a}.ndjson")

        seen = set()
        candidates_unique: List[Path] = []
        for c in candidates:
            c = Path(c)
            if str(c) in seen:
                continue
            seen.add(str(c))
            candidates_unique.append(c)

        for c in candidates_unique:
            rep = _load_report_any(c)
            if not rep:
                continue
            meta = rep.get('meta')
            if not meta:
                continue
            if not _args_match(meta, args.formats, libs, args.tie_threshold_pct):
                continue
            if not _signature_matches(meta, current_sig):
                continue

            # Cache hit
            rep = _reorient_report(rep, host_a, host_b)
            rep['meta'] = _build_meta(
                csv_path=args.csv,
                host_a=host_a,
                host_b=host_b,
                formats=args.formats,
                libs=libs,
                tie_threshold_pct=args.tie_threshold_pct,
                rows_effective=rows_effective,
                source='cache',
                source_report=str(c),
            )

            print(f"Cache: HIT (reusing {c})")

            _print_console(rep, tie_threshold_pct=args.tie_threshold_pct, quiet=args.quiet)

            # If user requested export, ensure target file exists with requested orientation/format.
            if args.json_out and not args.no_export:
                try:
                    target = Path(args.json_out)
                    same_target = False
                    try:
                        same_target = target.resolve() == Path(c).resolve()
                    except Exception:
                        same_target = str(target) == str(c)

                    if same_target:
                        print(f"\nReport already up-to-date at {target} (cache hit)")
                    else:
                        _export_report(rep, target, ndjson=args.ndjson)
                        print(f"\nReport saved to {target} ({'NDJSON' if args.ndjson else 'JSON'})")
                except Exception as e:
                    print(f"Failed to write report JSON: {e}")
            return 0

    if args.force:
        print("Cache: BYPASSED (--force)")
    else:
        print("Cache: MISS (recomputing)")

    result = {
        **compare_hosts(args.csv, host_a, host_b, remove_outliers=not args.keep_outliers),
        # Reuse the already loaded rows to avoid reading the CSV twice.
        'rows_all': rows,
    }
    meta = _build_meta(
        csv_path=args.csv,
        host_a=host_a,
        host_b=host_b,
        formats=args.formats,
        libs=libs,
        tie_threshold_pct=args.tie_threshold_pct,
        rows_effective=rows_effective,
        source='compute',
    )
    if not args.keep_outliers:
        meta['outliers_removed'] = True
    print_report(
        result,
        tie_threshold_pct=args.tie_threshold_pct,
        libs=libs,
        formats=args.formats,
        json_out=args.json_out,
        ndjson=args.ndjson,
        quiet=args.quiet,
        meta=meta,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
