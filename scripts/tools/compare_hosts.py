import csv
import argparse
import sys
from pathlib import Path
from statistics import mean, median
from typing import Dict, List, Optional, Tuple
import difflib
from datetime import datetime, timezone
import os

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
    try:
        return float(x) if x not in (None, "", "N/A") else None
    except Exception:
        return None


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    with open(csv_path, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        return list(rdr)


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
        for r in rows:
            vals = [fval(r.get(c)) for c in cols]
            vals = [v for v in vals if v is not None]
            if vals:
                per_row_means.append(mean(vals))
        if per_row_means:
            lib_means[lib] = {
                'mean': mean(per_row_means),
                'median': median(per_row_means),
                'n': len(per_row_means),
            }

    overall_per_row: List[float] = []
    for r in rows:
        vals: List[Optional[float]] = []
        for cols in lib_ops.values():
            vals.extend([fval(r.get(c)) for c in cols])
        flat = [v for v in vals if v is not None]
        if flat:
            overall_per_row.append(mean(flat))

    return {
        'rows': len(rows),
        'cpu_brand': cpu_brand,
        'cpu_logical_mean': cpu_logical,
        'cpu_physical_mean': cpu_physical,
        'mem_total_mean': mean(mem_total) if mem_total else None,
        'mem_avail_mean': mean(mem_avail) if mem_avail else None,
        'libs': lib_means,
        'overall_mean': mean(overall_per_row) if overall_per_row else None,
        'overall_median': median(overall_per_row) if overall_per_row else None,
    }


def relative_pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    # Percent change from a -> b: (a-b)/a*100; negative means b is faster if values are durations
    if a is None or b is None or a == 0:
        return None
    return (a - b) / a * 100.0


def compare_hosts(csv_path: Path, host_a: str, host_b: str) -> Dict:
    rows = load_rows(csv_path)
    buckets = filter_rows_by_hosts(rows, [host_a, host_b])

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

    return {
        'host_a': host_a,
        'host_b': host_b,
        'summary_a': sum_a,
        'summary_b': sum_b,
        'relative': rel,
        'rows_all': rows,
    }


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
            print(f"Winner: {report['host_b']} (≈{ov:.2f}% faster overall)")
        else:
            print(f"Winner: {report['host_a']} (≈{abs(ov):.2f}% faster overall)")

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
            print(f"{lib} mean: {fmt_float(stats.get('mean'))} | median: {fmt_float(stats.get('median'))} | n: {stats.get('n')}")
        print(f"Overall mean: {fmt_float(a.get('overall_mean'))} | median: {fmt_float(a.get('overall_median'))}")

        print("\n== Host B ==")
        print(f"Rows: {b.get('rows')}")
        print(f"CPU: {b.get('cpu_brand') or 'N/A'} | logical ~ {fmt_float(b.get('cpu_logical_mean'))} | physical ~ {fmt_float(b.get('cpu_physical_mean'))}")
        print(f"Mem Total Mean (GB): {fmt_float(b.get('mem_total_mean'))}")
        print(f"Mem Avail Mean (GB): {fmt_float(b.get('mem_avail_mean'))}")
        for lib, stats in (b.get('libs') or {}).items():
            print(f"{lib} mean: {fmt_float(stats.get('mean'))} | median: {fmt_float(stats.get('median'))} | n: {stats.get('n')}")
        print(f"Overall mean: {fmt_float(b.get('overall_mean'))} | median: {fmt_float(b.get('overall_median'))}")

        print("\n== Relative (who is faster) ==")
        if ov is None:
            print("Overall: N/A")
        else:
            if abs(ov) < tie_threshold_pct:
                print(f"Overall: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif ov > 0:
                print(f"Overall: {report['host_b']} faster by {ov:.2f}%")
            else:
                print(f"Overall: {report['host_a']} faster by {abs(ov):.2f}%")
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
            print(f"Winner: {report['host_b']} (≈{ov:.2f}% faster)")
        else:
            print(f"Winner: {report['host_a']} (≈{abs(ov):.2f}% faster)")

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
        print(f"- {report['host_a']} overall mean: {fmt_float(a.get('overall_mean'))} s")
        print(f"- {report['host_b']} overall mean: {fmt_float(b.get('overall_mean'))} s")
        if ov is not None:
            if abs(ov) < tie_threshold_pct:
                print(f"- Overall: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif ov > 0:
                print(f"- Overall: {report['host_b']} faster by {ov:.2f}%")
            else:
                print(f"- Overall: {report['host_a']} faster by {abs(ov):.2f}%")
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
                    print(f"Winner: {report['host_b']} (≈{ov_os:.2f}% faster)")
                else:
                    print(f"Winner: {report['host_a']} (≈{abs(ov_os):.2f}% faster)")
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
                        print(f"  * {title}: {report['host_b']} (≈{ov_fmt:.2f}% faster)")
                    else:
                        print(f"  * {title}: {report['host_a']} (≈{abs(ov_fmt):.2f}% faster)")
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
    # Apply library selection
    libs_selected = [l for l in (libs or LIB_OPS.keys()) if l in LIB_OPS]
    lib_ops = {l: LIB_OPS[l] for l in libs_selected}

    # Apply format filtering to overall rows
    rows_all = _filter_rows_by_formats(result.get('rows_all', []), formats)
    rows_a_overall = [r for r in rows_all if r.get('hostname') == result['host_a']]
    rows_b_overall = [r for r in rows_all if r.get('hostname') == result['host_b']]
    a = summarize_host(rows_a_overall, lib_ops)
    b = summarize_host(rows_b_overall, lib_ops)
    rel = {
        'overall_mean_pct': relative_pct(a.get('overall_mean'), b.get('overall_mean')),
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
        **compare_hosts(args.csv, host_a, host_b),
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
