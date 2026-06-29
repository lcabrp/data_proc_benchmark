import subprocess
import json
import sys
import os
from pathlib import Path

# Define constants and paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPARE_SCRIPT = PROJECT_ROOT / "scripts" / "tools" / "compare_hosts.py"
CSV_PATH = PROJECT_ROOT / "data" / "benchmark_results.csv"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
DATASET_SIZE = 10_000_000
VENV_PYTHON = PROJECT_ROOT / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
PYTHON_EXE = VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable)

# Define the base and contender laptops/desktops
my_base_laptop = "ZBookPowerG9-01"  # i7-12700H
contender_laptops = [
    "ZBookStudioG8",       # i7-11850H
    "ZBookPowerG10",      # i5-13600H
    "IdeaPadPro5i",       # Core Ultra 185H
    "IdeaPadPro5i-2",     # Core Ultra 285H
    "ZBookFuryG9-02",     # i9-12950HX
    "Legion7-16IRX9",      # i9-14900HX
    "ZBookPowerG9-02",    # i9-12800H
    "ZBookPowerG9-03",    # i7-12800H
    "Legion5-16IAX10"     # Ultra 9 275HX
]

my_base_desktop = "HP-Z2-G9"  # i9-12900
contender_desktops = [
    "OptPlex-7020-3",   # i5-14600
    "TS-P350-01"        # i7-11700
]

my_older_laptop = "IdeaPadS340"  # i7-1065G7
base_bussiness_laptop = "WL1111"   # i7-8656U
business_laptops = [
    "WL5022",          # i7-1185G7
    "WL5040",          # AMD Ryzen 8940HS
    "HP-EB830G6-01"     # i5-8365U
]

workstation_laptops = []

# Define the target and compare-to hosts
target = my_base_laptop
compare_to = contender_laptops

results = []
errors = []

# Ensure the results directory exists
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def fmt_gb(value: float) -> str:
    """Format a value in gigabytes with one decimal place."""
    return f"{value:.1f}GB" if isinstance(value, (int, float)) else "N/A"

def compare_hosts(target_host: str, contender_host: str) -> dict:
    """
    Compare the performance of two hosts using the specified script and dataset size.
    
    :param target_host: The name of the target host to compare against.
    :param contender_host: The name of the contender host to compare with.
    :return: A dictionary containing the comparison results.
    """
    json_path = RESULTS_DIR / f"compare_{target_host}_vs_{contender_host}.json"
    
    # Construct the command to run the comparison script
    cmd = [
        str(PYTHON_EXE),
        str(COMPARE_SCRIPT),
        "--csv", str(CSV_PATH),
        "--host", target_host,
        "--host", contender_host,
        "--dataset-size", str(DATASET_SIZE),
        "--json-out", str(json_path),
        "--force"
    ]
    
    try:
        # Run the comparison script
        subprocess.run(cmd, check=True, capture_output=True, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
        
        # Load the results from the JSON file
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        overall = data.get("overall", {})
        sum_a = overall.get("summary_a", {})
        sum_b = overall.get("summary_b", {})
        rel = overall.get("relative", {})
        host_a_name = data.get("host_a")
        host_b_name = data.get("host_b")
        
        # Determine which summary corresponds to the target and contender hosts
        if host_a_name == target_host:
            target_sum = sum_a
            host_sum = sum_b
        elif host_b_name == target_host:
            target_sum = sum_b
            host_sum = sum_a
        else:
            raise ValueError(f"Comparison output does not include target host {target_host!r}")
        
        os_filter = data.get("os_intersection_filter", {})
        common_os = os_filter.get("common_os") or []
        no_common_os = not common_os and bool(os_filter.get("applied"))
        pct = rel.get("overall_mean_pct")
        t_mean = target_sum.get("overall_mean")
        h_mean = host_sum.get("overall_mean")
        tie_thresh = data.get("tie_threshold_pct", 5.0)
        
        # Determine the winner based on the comparison results
        if no_common_os:
            winner = "No common OS"
        elif pct is None or t_mean is None or h_mean is None:
            winner = "N/A"
        elif abs(pct) < tie_thresh:
            winner = "Tie"
        elif t_mean < h_mean:
            winner = target_host  # target has lower mean time -> faster
        else:
            winner = contender_host  # compared host has lower mean time -> faster
        
        return {
            "host": contender_host,
            "host_cpu": host_sum.get("cpu_brand") or "N/A",
            "host_ram": host_sum.get("mem_total_mean", 0),
            "target_cpu": target_sum.get("cpu_brand") or "N/A",
            "target_ram": target_sum.get("mem_total_mean", 0),
            "winner": winner,
            "percent_diff": pct,
            "data": data
        }
    
    except (subprocess.CalledProcessError, OSError, json.JSONDecodeError, ValueError) as e:
        if isinstance(e, subprocess.CalledProcessError):
            message = e.stderr.decode("utf-8", errors="replace") if e.stderr else str(e)
        else:
            message = str(e)
        return {"host": contender_host, "error": message}

# Perform the comparisons
for host in compare_to:
    result = compare_hosts(target, host)
    if "error" in result:
        errors.append((host, result["error"]))
    else:
        results.append(result)

# Print the comparison results
for r in results:
    print(f"--- {r['host']} vs {target} ---")
    pct = r['percent_diff']
    abs_pct = abs(pct) if pct is not None else None
    diff_str = f"{abs_pct:.2f}%" if abs_pct is not None else "N/A"
    print(f"Host CPU: {r['host_cpu']} | RAM: {fmt_gb(r['host_ram'])}")
    print(f"Target CPU: {r['target_cpu']} | RAM: {fmt_gb(r['target_ram'])}")
    
    # Determine the direction of the comparison result
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
        direction = f"{target} is faster by {diff_str}"
    
    print(f"Winner: {direction}")
    
    # Print library performance differences if available
    libs_pct = r['data'].get("overall", {}).get("relative", {}).get("libs_pct", {})
    if libs_pct:
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

# Print any errors that occurred during comparisons
if errors:
    print(f"{len(errors)} comparison(s) failed.")
    for host, error in errors:
        print(f"Error comparing {host}: {error}")
