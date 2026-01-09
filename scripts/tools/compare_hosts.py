import csv
import argparse
import sys
from pathlib import Path
from statistics import mean, median
from typing import Dict, List, Optional, Tuple
import difflib

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


def print_report(result: Dict, tie_threshold_pct: float = 5.0, libs: Optional[List[str]] = None, formats: Optional[List[str]] = None, json_out: Optional[Path] = None, ndjson: bool = False, quiet: bool = False) -> None:
    a = result['summary_a']
    b = result['summary_b']
    rel = result['relative']

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

    report = {
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

    # Headline verdict (one-liner at the very top)
    ov = rel.get('overall_mean_pct')
    if ov is None:
        print("Winner: N/A")
    else:
        if abs(ov) < tie_threshold_pct:
            print(f"Winner: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif ov > 0:
            print(f"Winner: {result['host_b']} (≈{ov:.2f}% faster overall)")
        else:
            print(f"Winner: {result['host_a']} (≈{abs(ov):.2f}% faster overall)")

    # Summary block for quick scanning
    print("== Summary ==")
    if ov is None:
        print("- Overall: N/A")
    else:
        if abs(ov) < tie_threshold_pct:
            print(f"- Overall: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif ov > 0:
            print(f"- Overall: {result['host_b']} faster by {ov:.2f}%")
        else:
            print(f"- Overall: {result['host_a']} faster by {abs(ov):.2f}%")
    libs_list_summary = [l for l in ['pandas', 'polars', 'duckdb', 'fireducks'] if l in lib_ops]
    for lib in libs_list_summary:
        p = rel['libs_pct'].get(lib)
        if p is None:
            print(f"- {lib}: N/A")
            continue
        if abs(p) < tie_threshold_pct:
            print(f"- {lib}: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif p > 0:
            print(f"- {lib}: {result['host_b']} faster by {p:.2f}%")
        else:
            print(f"- {lib}: {result['host_a']} faster by {abs(p):.2f}%")
    # Memory quick compare
    if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
        mem_a = a['mem_avail_mean']
        mem_b = b['mem_avail_mean']
        if abs(mem_a - mem_b) < 1e-9:
            print(f"- Memory: Similar available RAM (~{mem_a:.2f} GB)")
        elif mem_b > mem_a:
            print(f"- Memory: {result['host_b']} higher available RAM ({mem_b:.2f} GB vs {mem_a:.2f} GB)")
        else:
            print(f"- Memory: {result['host_a']} higher available RAM ({mem_a:.2f} GB vs {mem_b:.2f} GB)")
    print("")

    if not quiet:
        print("== Host A ==")
        print(f"Rows: {a['rows']}")
        print(f"CPU: {a.get('cpu_brand') or 'N/A'} | logical ~ {fmt_float(a.get('cpu_logical_mean'))} | physical ~ {fmt_float(a.get('cpu_physical_mean'))}")
        print(f"Mem Total Mean (GB): {fmt_float(a['mem_total_mean'])}")
        print(f"Mem Avail Mean (GB): {fmt_float(a['mem_avail_mean'])}")
        for lib, stats in a['libs'].items():
            print(f"{lib} mean: {fmt_float(stats['mean'])} | median: {fmt_float(stats['median'])} | n: {stats['n']}")
        print(f"Overall mean: {fmt_float(a['overall_mean'])} | median: {fmt_float(a['overall_median'])}")

    if not quiet:
        print("\n== Host B ==")
        print(f"Rows: {b['rows']}")
        print(f"CPU: {b.get('cpu_brand') or 'N/A'} | logical ~ {fmt_float(b.get('cpu_logical_mean'))} | physical ~ {fmt_float(b.get('cpu_physical_mean'))}")
        print(f"Mem Total Mean (GB): {fmt_float(b['mem_total_mean'])}")
        print(f"Mem Avail Mean (GB): {fmt_float(b['mem_avail_mean'])}")
        for lib, stats in b['libs'].items():
            print(f"{lib} mean: {fmt_float(stats['mean'])} | median: {fmt_float(stats['median'])} | n: {stats['n']}")
        print(f"Overall mean: {fmt_float(b['overall_mean'])} | median: {fmt_float(b['overall_median'])}")

    if not quiet:
        print("\n== Relative (who is faster) ==")
        ov_rel = rel.get('overall_mean_pct')
        if ov_rel is None:
            print("Overall: N/A")
        else:
            if abs(ov_rel) < tie_threshold_pct:
                print(f"Overall: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif ov_rel > 0:
                print(f"Overall: {result['host_b']} faster by {ov_rel:.2f}%")
            else:
                print(f"Overall: {result['host_a']} faster by {abs(ov_rel):.2f}%")
        for lib in lib_ops.keys():
            pct = rel['libs_pct'].get(lib)
            if pct is None:
                print(f"{lib}: N/A")
                continue
            if abs(pct) < tie_threshold_pct:
                print(f"{lib}: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif pct > 0:
                print(f"{lib}: {result['host_b']} faster by {pct:.2f}%")
            else:
                print(f"{lib}: {result['host_a']} faster by {abs(pct):.2f}%")

    # Verdict / Conclusion block
    print("\n== Verdict ==")
    # Winner/Tie logic
    ov = rel.get('overall_mean_pct')
    if ov is not None:
        if abs(ov) < tie_threshold_pct:
            print(f"Winner: Tie (within {tie_threshold_pct:.1f}% threshold)")
        elif ov > 0:
            print(f"Winner: {result['host_b']} (≈{ov:.2f}% faster)")
        else:
            print(f"Winner: {result['host_a']} (≈{abs(ov):.2f}% faster)")
    # Bottom Line
    print("Bottom Line")
    overall_pct = rel.get('overall_mean_pct')
    if overall_pct is not None:
        faster = "faster" if overall_pct > 0 else "slower"
        print(f"- {result['host_b']} vs {result['host_a']} (overall): about {abs(overall_pct):.2f}% {faster} on average across all ops.")
    # By library (include all selected libs, including fireducks when present)
    lib_lines = []
    libs_list_bottom = [l for l in ['pandas', 'polars', 'duckdb', 'fireducks'] if l in lib_ops]
    for lib in libs_list_bottom:
        p = rel['libs_pct'].get(lib)
        if p is not None:
            faster = "faster" if p > 0 else "slower"
            lib_lines.append(f"{lib} ~{abs(p):.2f}% {faster}")
    if lib_lines:
        print(f"- By library: {'; '.join(lib_lines)}.")
    # Memory
    if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
        mem_a = a['mem_avail_mean']
        mem_b = b['mem_avail_mean']
        if abs(mem_a - mem_b) < 1e-9:
            print(f"- Memory: Both show similar average available RAM (~{mem_a:.2f} GB).")
        elif mem_b > mem_a:
            print(f"- Memory: {result['host_b']} shows higher average available RAM ({mem_b:.2f} GB vs {mem_a:.2f} GB).")
        else:
            print(f"- Memory: {result['host_a']} shows higher average available RAM ({mem_a:.2f} GB vs {mem_b:.2f} GB).")

    # Methodology
    if not quiet:
        print("\nMethodology")
        print("- Compared mean operation times per host across four ops: filter_group, statistics, complex_join, timeseries.")
        print("- Aggregated per library (pandas, polars, duckdb, fireducks where available), then averaged across ops.")
        print("- Report includes per-OS and per-format (e.g., CSV, Parquet) breakdowns when both hosts have data.")

    # Key Numbers
    if not quiet:
        print("\nKey Numbers")
        print(f"- {result['host_a']} overall mean: {fmt_float(a['overall_mean'])} s")
        print(f"- {result['host_b']} overall mean: {fmt_float(b['overall_mean'])} s")
        if overall_pct is not None:
            if abs(overall_pct) < tie_threshold_pct:
                print(f"- Overall: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif overall_pct > 0:
                print(f"- Overall: {result['host_b']} faster by {overall_pct:.2f}%")
            else:
                print(f"- Overall: {result['host_a']} faster by {abs(overall_pct):.2f}%")
        for lib in [l for l in ['pandas', 'polars', 'duckdb', 'fireducks'] if l in lib_ops]:
            p = rel['libs_pct'].get(lib)
            if p is None:
                print(f"- {lib}: N/A")
                continue
            if abs(p) < tie_threshold_pct:
                print(f"- {lib}: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif p > 0:
                print(f"- {lib}: {result['host_b']} faster by {p:.2f}%")
            else:
                print(f"- {lib}: {result['host_a']} faster by {abs(p):.2f}%")
    if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
        print(f"- Average memory available: {result['host_a']} ~ {a['mem_avail_mean']:.2f} GB; {result['host_b']} ~ {b['mem_avail_mean']:.2f} GB")

    # Interpretation
    if not quiet:
        print("\nInterpretation")
        a_log = a.get('cpu_logical_mean')
        a_phy = a.get('cpu_physical_mean')
        b_log = b.get('cpu_logical_mean')
        b_phy = b.get('cpu_physical_mean')
        print(f"- {result['host_b']}'s {b.get('cpu_brand') or 'CPU'} ({fmt_float(b_log,0)} logical / {fmt_float(b_phy,0)} physical) vs {result['host_a']}'s {a.get('cpu_brand') or 'CPU'} ({fmt_float(a_log,0)} / {fmt_float(a_phy,0)}).")
        print("- Advantages hold across CSV and Parquet; Parquet tends to be faster overall, but relative differences persist.")
        if a.get('mem_avail_mean') is not None and b.get('mem_avail_mean') is not None:
            print("- Higher available RAM on the faster host adds headroom for larger datasets and complex joins.")

    # Per-OS breakdown
    os_list = _os_values_for_hosts(rows_all, result['host_a'], result['host_b'])
    if os_list and not quiet:
        print("\n== By OS ==")
        for os_name in os_list:
            print(f"\n-- {os_name} --")
            rows_a_os = _filter_rows_for_host_os(rows_all, result['host_a'], os_name)
            rows_b_os = _filter_rows_for_host_os(rows_all, result['host_b'], os_name)
            sum_a_os = summarize_host(rows_a_os, lib_ops)
            sum_b_os = summarize_host(rows_b_os, lib_ops)
            rel_os = {
                'overall_mean_pct': relative_pct(sum_a_os.get('overall_mean'), sum_b_os.get('overall_mean')),
                'libs_pct': {l: relative_pct(sum_a_os.get('libs', {}).get(l, {}).get('mean'), sum_b_os.get('libs', {}).get(l, {}).get('mean')) for l in lib_ops.keys()}
            }
            for lib in LIB_OPS.keys():
                a_mean = sum_a_os.get('libs', {}).get(lib, {}).get('mean')
                b_mean = sum_b_os.get('libs', {}).get(lib, {}).get('mean')
                rel_os['libs_pct'][lib] = relative_pct(a_mean, b_mean)

            ov_os = rel_os.get('overall_mean_pct')
            if ov_os is None or sum_a_os.get('rows', 0) == 0 or sum_b_os.get('rows', 0) == 0:
                print("Insufficient data to compare.")
                continue
            # Winner/Tie per OS
            if abs(ov_os) < tie_threshold_pct:
                print(f"Winner: Tie (within {tie_threshold_pct:.1f}% threshold)")
            elif ov_os > 0:
                print(f"Winner: {result['host_b']} (≈{ov_os:.2f}% faster)")
            else:
                print(f"Winner: {result['host_a']} (≈{abs(ov_os):.2f}% faster)")
            print(f"- Overall mean: {result['host_a']} {fmt_float(sum_a_os.get('overall_mean'))} s vs {result['host_b']} {fmt_float(sum_b_os.get('overall_mean'))} s")
            # Per-lib concise winner/tie lines
            lib_lines = []
            libs_list = [l for l in ['pandas','polars','duckdb','fireducks'] if l in lib_ops]
            for lib in libs_list:
                p = rel_os['libs_pct'].get(lib)
                if p is None:
                    lib_lines.append(f"{lib} N/A")
                elif abs(p) < tie_threshold_pct:
                    lib_lines.append(f"{lib} Tie")
                elif p > 0:
                    lib_lines.append(f"{lib} {result['host_b']} faster by {p:.2f}%")
                else:
                    lib_lines.append(f"{lib} {result['host_a']} faster by {abs(p):.2f}%")
            if lib_lines:
                print("- Per-library:", '; '.join(lib_lines))

            os_entry = {
                'os': os_name,
                'summary_a': sum_a_os,
                'summary_b': sum_b_os,
                'relative': rel_os,
                'by_format': []
            }
            report['by_os'].append(os_entry)

            # Per-format breakdown within OS
            fmts = _formats_for_hosts_os(rows_all, result['host_a'], result['host_b'], os_name)
            if formats:
                fmts = [f for f in fmts if f.lower() in {x.lower(): x.lower() for x in formats}]
            if fmts:
                for fmt in fmts:
                    rows_a_os_fmt = _filter_rows_for_host_os_fmt(rows_all, result['host_a'], os_name, fmt)
                    rows_b_os_fmt = _filter_rows_for_host_os_fmt(rows_all, result['host_b'], os_name, fmt)
                    sum_a_os_fmt = summarize_host(rows_a_os_fmt, lib_ops)
                    sum_b_os_fmt = summarize_host(rows_b_os_fmt, lib_ops)
                    rel_os_fmt = {
                        'overall_mean_pct': relative_pct(sum_a_os_fmt.get('overall_mean'), sum_b_os_fmt.get('overall_mean')),
                        'libs_pct': {l: relative_pct(sum_a_os_fmt.get('libs', {}).get(l, {}).get('mean'), sum_b_os_fmt.get('libs', {}).get(l, {}).get('mean')) for l in lib_ops.keys()}
                    }
                    for lib in LIB_OPS.keys():
                        a_mean = sum_a_os_fmt.get('libs', {}).get(lib, {}).get('mean')
                        b_mean = sum_a_os_fmt.get('libs', {}).get(lib, {}).get('mean') if False else sum_b_os_fmt.get('libs', {}).get(lib, {}).get('mean')
                        rel_os_fmt['libs_pct'][lib] = relative_pct(a_mean, b_mean)

                    ov_os_fmt = rel_os_fmt.get('overall_mean_pct')
                    if ov_os_fmt is None or sum_a_os_fmt.get('rows', 0) == 0 or sum_b_os_fmt.get('rows', 0) == 0:
                        continue
                    title = fmt.upper() if fmt else fmt
                    if abs(ov_os_fmt) < tie_threshold_pct:
                        print(f"  * {title}: Tie (within {tie_threshold_pct:.1f}% threshold)")
                    elif ov_os_fmt > 0:
                        print(f"  * {title}: {result['host_b']} (≈{ov_os_fmt:.2f}% faster)")
                    else:
                        print(f"  * {title}: {result['host_a']} (≈{abs(ov_os_fmt):.2f}% faster)")
                    print(f"    - Overall mean: {result['host_a']} {fmt_float(sum_a_os_fmt.get('overall_mean'))} s vs {result['host_b']} {fmt_float(sum_b_os_fmt.get('overall_mean'))} s")
                    lib_lines_fmt = []
                    libs_list_fmt = [l for l in ['pandas','polars','duckdb','fireducks'] if l in lib_ops]
                    for lib in libs_list_fmt:
                        p = rel_os_fmt['libs_pct'].get(lib)
                        if p is None:
                            lib_lines_fmt.append(f"{lib} N/A")
                        elif abs(p) < tie_threshold_pct:
                            lib_lines_fmt.append(f"{lib} Tie")
                        elif p > 0:
                            lib_lines_fmt.append(f"{lib} {result['host_b']} faster by {p:.2f}%")
                        else:
                            lib_lines_fmt.append(f"{lib} {result['host_a']} faster by {abs(p):.2f}%")
                    if lib_lines_fmt:
                        print("    - Per-library:", '; '.join(lib_lines_fmt))

                    os_entry['by_format'].append({
                        'format': fmt,
                        'summary_a': sum_a_os_fmt,
                        'summary_b': sum_b_os_fmt,
                        'relative': rel_os_fmt,
                    })

    # Optional JSON/NDJSON export
    if json_out:
        try:
            import json
            json_out = Path(json_out)
            json_out.parent.mkdir(parents=True, exist_ok=True)
            if ndjson:
                with open(json_out, 'w', encoding='utf-8') as f:
                    # Write overall
                    f.write(json.dumps({'type': 'overall', **report['overall'], 'host_a': report['host_a'], 'host_b': report['host_b']}) + '\n')
                    # Write per OS and per format
                    for os_entry in report['by_os']:
                        base = {'type': 'os', 'os': os_entry['os'], 'host_a': report['host_a'], 'host_b': report['host_b']}
                        f.write(json.dumps({**base, 'summary_a': os_entry['summary_a'], 'summary_b': os_entry['summary_b'], 'relative': os_entry['relative']}) + '\n')
                        for fmt_entry in os_entry['by_format']:
                            base_fmt = {'type': 'os_format', 'os': os_entry['os'], 'format': fmt_entry['format'], 'host_a': report['host_a'], 'host_b': report['host_b']}
                            f.write(json.dumps({**base_fmt, 'summary_a': fmt_entry['summary_a'], 'summary_b': fmt_entry['summary_b'], 'relative': fmt_entry['relative']}) + '\n')
            else:
                with open(json_out, 'w', encoding='utf-8') as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
            print(f"\nReport saved to {json_out} ({'NDJSON' if ndjson else 'JSON'})")
        except Exception as e:
            print(f"Failed to write report JSON: {e}")


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

    args = parser.parse_args(argv)
    if len(args.host) != 2:
        print('Please provide exactly two --host arguments')
        return 2

    host_a, host_b = args.host
    # Validate exact hostnames exist in CSV
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

    result = compare_hosts(args.csv, host_a, host_b)
    libs = None
    if args.libs:
        libs = [x.strip().lower() for x in args.libs.split(',') if x.strip()]
    print_report(result, tie_threshold_pct=args.tie_threshold_pct, libs=libs, formats=args.formats, json_out=args.json_out, ndjson=args.ndjson, quiet=args.quiet)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
