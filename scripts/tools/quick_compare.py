"""
quick_compare.py - Compare a host against one or more other hosts.

Looks for cached results in data/results/ first (checking both A_vs_B and
B_vs_A filename orientations) before calling compare_hosts.py.

Usage:
    python scripts/tools/quick_compare.py --host IdeaPadPro5i-2 --vs Legion7-16IRX9 IdeaPadPro5i
    python scripts/tools/quick_compare.py --host HP-ZB-Fury-G10 --vs ZBookFuryG9 ZBookFuryG8 Precision-7770
    python scripts/tools/quick_compare.py --host ZBookPowerG9 --vs HP-ZB-Power-G10 --results-dir data/custom_results
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_cached(target: str, host: str, results_dir: Path) -> tuple[Path | None, bool]:
    """Return (json_path, is_reversed) for an existing cache file, or (None, False)."""
    forward = results_dir / f"compare_{target}_vs_{host}.json"
    backward = results_dir / f"compare_{host}_vs_{target}.json"
    if forward.exists():
        return forward, False
    if backward.exists():
        return backward, True
    return None, False


def _run_compare(target: str, host: str, results_dir: Path, csv: Path, force: bool) -> Path | None:
    """Run compare_hosts.py and return the path of the written JSON, or None on failure."""
    script = Path(__file__).parent / "compare_hosts.py"
    out_file = results_dir / f"compare_{target}_vs_{host}.json"
    cmd = [
        sys.executable, str(script),
        "--csv", str(csv),
        "--host", target,
        "--host", host,
        "--json-out", str(out_file),
    ]
    if force:
        cmd.append("--force")
    try:
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        return out_file
    except subprocess.CalledProcessError as exc:
        err = exc.stderr.decode("utf-8", errors="replace")
        print(f"  [ERROR] compare_hosts.py failed for {host}:\n{err}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Report printing
# ---------------------------------------------------------------------------

def _print_comparison(target: str, host: str, data: dict) -> None:
    """Pretty-print one comparison."""
    overall = data.get("overall", {})
    sum_a = overall.get("summary_a", {})
    sum_b = overall.get("summary_b", {})
    rel = overall.get("relative", {})

    host_a = data.get("host_a", "")
    target_sum = sum_a if host_a == target else sum_b
    host_sum   = sum_b if host_a == target else sum_a

    pct        = rel.get("overall_mean_pct", 0) or 0
    t_mean     = target_sum.get("overall_mean", 0)
    h_mean     = host_sum.get("overall_mean", 0)
    tie_thresh = data.get("tie_threshold_pct", 5.0)

    if abs(pct) < tie_thresh:
        winner = "Tie"
        verdict = f"~{pct:.2f}% difference (within tie threshold)"
    elif t_mean < h_mean:
        winner = target
        verdict = f"{target} is {abs(pct):.2f}% faster overall"
    else:
        winner = host
        verdict = f"{host} is {abs(pct):.2f}% faster overall"

    sep = "=" * 60
    print(f"\n{sep}")
    print(f"  {target}  vs  {host}")
    print(sep)
    print(f"  Target : {target_sum.get('cpu_brand', 'Unknown')}  |  RAM: {target_sum.get('mem_total_mean', 0):.1f} GB")
    print(f"  Opponent: {host_sum.get('cpu_brand', 'Unknown')}  |  RAM: {host_sum.get('mem_total_mean', 0):.1f} GB")
    print(f"  Winner  : {winner}")
    print(f"  Verdict : {verdict}")

    libs = rel.get("libs_pct", {})
    if libs:
        print()
        print("  Library breakdown (positive = opponent is faster, negative = target is faster):")
        for lib, val in libs.items():
            if val is None:
                print(f"    {lib:12s}: N/A")
            else:
                direction = f"{host} +{val:.2f}%" if val >= 0 else f"{target} +{abs(val):.2f}%"
                print(f"    {lib:12s}: {direction}")

    # OS breakdown summary if available
    by_os = data.get("by_os", [])
    if by_os:
        print()
        print("  Per-OS breakdown:")
        for os_block in by_os:
            os_name = os_block.get("os", "?")
            os_rel  = os_block.get("relative", {})
            os_pct  = os_rel.get("overall_mean_pct")
            if os_pct is None:
                continue
            os_sum_a = os_block.get("summary_a", {})
            os_sum_b = os_block.get("summary_b", {})
            os_t_mean = os_sum_a.get("overall_mean", 0) if host_a == target else os_sum_b.get("overall_mean", 0)
            os_h_mean = os_sum_b.get("overall_mean", 0) if host_a == target else os_sum_a.get("overall_mean", 0)
            if abs(os_pct) < tie_thresh:
                os_winner = "Tie"
            elif os_t_mean < os_h_mean:
                os_winner = target
            else:
                os_winner = host
            print(f"    {os_name:10s}: {os_winner} by {abs(os_pct):.2f}%")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Quickly compare a host against one or more others, using cached results when available.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--host",        required=True,               help="The primary host to benchmark.")
    parser.add_argument("--vs",          required=True, nargs="+",    help="One or more opponent hosts to compare against.")
    parser.add_argument("--csv",         type=Path, default=Path("data/benchmark_results.csv"),
                        help="Path to the benchmark results CSV (default: data/benchmark_results.csv).")
    parser.add_argument("--results-dir", type=Path, default=Path("data/results"),
                        help="Directory to search for cached comparisons and save new ones (default: data/results).")
    parser.add_argument("--force",       action="store_true",
                        help="Ignore cached files and recompute all comparisons.")
    args = parser.parse_args()

    target      = args.host.strip(". ,")
    opponents   = [h.strip(". ,") for h in args.vs]

    # Warn if any hostname was silently cleaned up
    if target != args.host:
        print(f"[WARNING] --host had trailing punctuation, interpreted as: {target!r}")
    for raw, cleaned in zip(args.vs, opponents):
        if raw != cleaned:
            print(f"[WARNING] hostname {raw!r} had trailing punctuation, interpreted as: {cleaned!r}")
    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    print(f"Comparing: {target}  vs  {', '.join(opponents)}")
    print(f"Cache dir: {results_dir}")

    for host in opponents:
        cached_path, is_reversed = _find_cached(target, host, results_dir)

        if cached_path and not args.force:
            source = "cache"
            json_path = cached_path
        else:
            if args.force:
                source = "recomputed (--force)"
            elif cached_path is None:
                source = "computed (no cache found)"
            json_path = _run_compare(target, host, results_dir, args.csv, force=args.force)
            is_reversed = False  # freshly written file always has target as host_a

        if json_path is None or not json_path.exists():
            print(f"\n[SKIP] Could not obtain comparison for {host}.")
            continue

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        print(f"\n  [{host}] source: {source} ({json_path.name})")
        _print_comparison(target, host, data)

    print()


if __name__ == "__main__":
    main()
