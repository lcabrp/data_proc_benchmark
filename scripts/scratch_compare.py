import subprocess
import json
import sys
import os
import csv
import difflib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPARE_SCRIPT = PROJECT_ROOT / "scripts" / "tools" / "compare_hosts.py"
CSV_PATH = PROJECT_ROOT / "data" / "benchmark_results.csv"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
DATASET_SIZE = 10_000_000
VENV_PYTHON = PROJECT_ROOT / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
PYTHON_EXE = VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable)


def load_hostnames(csv_path: Path):
    """Load unique hostnames from the benchmark CSV."""
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return sorted({(row.get("hostname") or "").strip() for row in reader if (row.get("hostname") or "").strip()})


def resolve_hostname(name: str, known_hosts):
    """Resolve a host by exact match first, then case-insensitive match."""
    if name in known_hosts:
        return name, None

    lowered = name.lower()
    ci_matches = [h for h in known_hosts if h.lower() == lowered]
    if len(ci_matches) == 1:
        return ci_matches[0], f"Normalized host '{name}' -> '{ci_matches[0]}'"
    if len(ci_matches) > 1:
        return None, f"Ambiguous host '{name}': matches {ci_matches}"

    hints = difflib.get_close_matches(name, known_hosts, n=5, cutoff=0.5)
    if hints:
        return None, f"Unknown host '{name}'. Did you mean: {', '.join(hints)}"
    return None, f"Unknown host '{name}'"

my_base_laptop = "ZBookPowerG9-02" # i7-12800H
contender_laptops = [
    "ZBookStudioG8" # i7-11850H
    , "ZBookPowerG10" # i5-13600H
    , "IdeaPadPro5i" # Core Ultra 185H
    , "IdeaPadPro5i-2" # Core Ultra 285H
    , "ZBookFuryG9-02" # i9-12950HX
    , "Legion7-16IRX9" # i9-14900HX
    , "ZBookPowerG9-01"  # i9-12700H
    , "ZBookPowerG9-03" # i7-12800H
    , "Legion5-16IAX10" # Ultra 9 275HX
]

my_base_desktop = "HP-Z2-G9" # i9-12900
contender_desktops = [
    "OptPlex-7020-3" # i5-14600
    ,"TS-P350-01" # i7-11700
]

my_older_laptop = "IdeaPadS340" # i7-1065G7
base_bussiness_laptop = "WL1111" # i7-8656U
business_laptops = [
    "WL5022" # i7-1185G7
    , "WL5040" # AMD Ryzen 8940HS
    , "HP-EB830G6-01" #i5-8365U
    , "HP-EB-G6-SJ" # i5-8365U
    , "WL1111" # i7-8665U

]
workstation_laptops = []


target = my_older_laptop
compare_to = business_laptops

results = []
errors = []
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

available_hosts = load_hostnames(CSV_PATH)
target_resolved, target_message = resolve_hostname(target, available_hosts)
if target_resolved is None:
    print(f"Error: {target_message}")
    sys.exit(1)
if target_message:
    print(target_message)

normalized_hosts = []
for host in compare_to:
    resolved_host, host_message = resolve_hostname(host, available_hosts)
    if resolved_host is None:
        errors.append((host, host_message))
        print(f"Error comparing {host}: {host_message}")
        continue
    if host_message:
        print(host_message)
    if resolved_host == target_resolved:
        print(f"Skipping self-comparison for host '{resolved_host}'")
        continue
    normalized_hosts.append((host, resolved_host))

for requested_host, resolved_host in normalized_hosts:
    json_path = RESULTS_DIR / f"compare_{target_resolved}_vs_{resolved_host}.json"
    # Use --since 2026-01-01 to prevent historical 2025 runs (which may contain outdated library versions
    # or system anomalies) from contaminating active 2026 benchmark comparisons.
    cmd = [
        str(PYTHON_EXE),
        str(COMPARE_SCRIPT),
        "--csv", str(CSV_PATH),
        "--host", target_resolved,
        "--host", resolved_host,
        "--dataset-size", str(DATASET_SIZE),
        # "--since", "2026-01-01",  # Not needed for now since I deleted older results
        "--json-out", str(json_path),
        "--force"
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
        
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            
        overall = data.get("overall", {})
        sum_a = overall.get("summary_a", {})
        sum_b = overall.get("summary_b", {})
        rel = overall.get("relative", {})
        
        host_a_name = data.get("host_a")
        host_b_name = data.get("host_b")
        
        if host_a_name == target_resolved:
            target_sum = sum_a
            host_sum = sum_b
        elif host_b_name == target_resolved:
            target_sum = sum_b
            host_sum = sum_a
        else:
            raise ValueError(f"Comparison output does not include target host {target_resolved!r}")

        os_filter = data.get("os_intersection_filter", {})
        common_os = os_filter.get("common_os") or []
        no_common_os = not common_os and bool(os_filter.get("applied"))
        
        pct = rel.get("overall_mean_pct")
        
        t_mean = target_sum.get("overall_mean")
        h_mean = host_sum.get("overall_mean")
        
        tie_thresh = data.get("tie_threshold_pct", 5.0)
        if no_common_os:
            winner = "No common OS"
        elif pct is None or t_mean is None or h_mean is None:
            winner = "N/A"
        elif abs(pct) < tie_thresh:
            winner = "Tie"
        elif t_mean < h_mean:
            winner = target_resolved  # target has lower mean time -> faster
        else:
            winner = resolved_host  # compared host has lower mean time -> faster
            
        results.append({
            "host": resolved_host,
            "requested_host": requested_host,
            "host_cpu": host_sum.get("cpu_brand") or "N/A",
            "host_ram": host_sum.get("mem_total_mean", 0),
            "target_cpu": target_sum.get("cpu_brand") or "N/A",
            "target_ram": target_sum.get("mem_total_mean", 0),
            "winner": winner,
            "percent_diff": pct,
            "data": data
        })
    except (subprocess.CalledProcessError, OSError, json.JSONDecodeError, ValueError) as e:
        if isinstance(e, subprocess.CalledProcessError):
            message = e.stderr.decode("utf-8", errors="replace") if e.stderr else str(e)
        else:
            message = str(e)
        errors.append((requested_host, message))
        print(f"Error comparing {requested_host}: {message}")


def fmt_gb(value):
    return f"{value:.1f}GB" if isinstance(value, (int, float)) else "N/A"


for r in results:
    host_label = r['host']
    if r.get('requested_host') and r.get('requested_host') != r['host']:
        host_label = f"{r['host']} (requested: {r['requested_host']})"
    print(f"--- {host_label} vs {target_resolved} ---")
    pct = r['percent_diff']
    abs_pct = abs(pct) if pct is not None else None
    diff_str = f"{abs_pct:.2f}%" if abs_pct is not None else "N/A"
    print(f"Host CPU: {r['host_cpu']} | RAM: {fmt_gb(r['host_ram'])}")
    print(f"Target CPU: {r['target_cpu']} | RAM: {fmt_gb(r['target_ram'])}")
    # pct = (target_mean - host_mean) / target_mean * 100
    # positive -> host is faster; negative -> host is slower
    if r['winner'] == "Tie":
        direction = f"Tie (within {diff_str})"
    elif r['winner'] == "No common OS":
        os_filter = r['data'].get("os_intersection_filter", {})
        direction = os_filter.get("reason") or "No shared OS environments"
    elif r['winner'] == "N/A":
        direction = "N/A"
    elif pct is not None and pct > 0:
        direction = f"{r['host']} is faster by {diff_str}"
    else:
        direction = f"{target_resolved} is faster by {diff_str}"
    print(f"Winner: {direction}")
    
    libs_pct = r['data'].get("overall", {}).get("relative", {}).get("libs_pct", {})
    if libs_pct:
        # positive diff -> host is faster; negative diff -> host is slower
        def fmt_lib(v):
            if v is None:
                return "N/A"
            return f"{'+' if v >= 0 else ''}{v:.2f}% ({'host faster' if v > 0 else 'host slower' if v < 0 else 'same'})"
        print(f"  Pandas diff: {fmt_lib(libs_pct.get('pandas'))}")
        print(f"  Polars diff: {fmt_lib(libs_pct.get('polars'))}")
        print(f"  DuckDB diff: {fmt_lib(libs_pct.get('duckdb'))}")
        if 'fireducks' in libs_pct:
            print(f"  FireDucks diff: {fmt_lib(libs_pct.get('fireducks'))}")
    
    print("")

if errors:
    print(f"{len(errors)} comparison(s) failed.")
    sys.exit(1)
