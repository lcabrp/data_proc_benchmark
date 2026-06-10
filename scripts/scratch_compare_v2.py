# Refactored with Qwen2.5-Coder 14B

import subprocess
import json
import sys
import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPARE_SCRIPT = PROJECT_ROOT / "scripts" / "tools" / "compare_hosts.py"
CSV_PATH = PROJECT_ROOT / "data" / "benchmark_results.csv"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
DATASET_SIZE = 10_000_000
VENV_PYTHON = PROJECT_ROOT / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
PYTHON_EXE = VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable)

my_base_laptop = "ZBookPowerG9-02"  # i7-12800H
contender_laptops = [
    "ZBookStudioG8",  # i7-11850H
    "ZBookPowerG10",  # i5-13600H
    "IdeaPadPro5i-2",  # Core Ultra 285H
    "IdeaPadPro5i",  # Core Ultra 185H
    "ZBookFuryG9-02",  # i9-12950HX
    "Legion7-16IRX9",  # i9-14900HX
    "ZBookPowerG9-01",  # i9-12700H
    "ZBookPowerG9-03"  # i7-12800H
]

my_base_desktop = "HP-Z2-G9"  # i9-12900
contender_desktops = [
    "OptPlex-7020-3",  # i5-14600
    "TS-P350-01"  # i7-11700
]

my_older_laptop = "IdeaPadS340"  # i7-1065G7
base_bussiness_laptop = "WL1111"  # i7-8656U
business_laptops = [
    "WL5022",  # i7-1185G7
    "WL5040"  # AMD Ryzen 8940HS
]

workstation_laptops = []

target = my_base_laptop
compare_to = contender_laptops

results = []
errors = []
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def run_comparison(target, host):
    json_path = RESULTS_DIR / f"compare_{target}_vs_{host}.json"
    cmd = [
        str(PYTHON_EXE),
        str(COMPARE_SCRIPT),
        "--csv", str(CSV_PATH),
        "--host", target,
        "--host", host,
        "--dataset-size", str(DATASET_SIZE),
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
        
        if host_a_name == target:
            target_sum = sum_a
            host_sum = sum_b
        elif host_b_name == target:
            target_sum = sum_b
            host_sum = sum_a
        else:
            raise ValueError(f"Comparison output does not include target host {target!r}")
        
        pct = rel.get("overall_mean_pct")
        
        t_mean = target_sum.get("overall_mean")
        h_mean = host_sum.get("overall_mean")
        
        tie_thresh = data.get("tie_threshold_pct", 5.0)
        if pct is None or t_mean is None or h_mean is None:
            winner = "N/A"
        elif abs(pct) < tie_thresh:
            winner = "Tie"
        elif t_mean < h_mean:
            winner = target  # target has lower mean time -> faster
        else:
            winner = host  # compared host has lower mean time -> faster
            
        return {
            "host": host,
            "host_cpu": host_sum.get("cpu_brand", "Unknown"),
            "host_ram": host_sum.get("mem_total_mean", 0),
            "target_cpu": target_sum.get("cpu_brand", "Unknown"),
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
        logging.error(f"Error comparing {host}: {message}")
        return None

def fmt_gb(value):
    return f"{value:.1f}GB" if isinstance(value, (int, float)) else "N/A"

for host in compare_to:
    result = run_comparison(target, host)
    if result:
        results.append(result)

for r in results:
    logging.info(f"--- {r['host']} vs {target} ---")
    pct = r['percent_diff']
    abs_pct = abs(pct) if pct is not None else None
    diff_str = f"{abs_pct:.2f}%" if abs_pct is not None else "N/A"
    logging.info(f"Host CPU: {r['host_cpu']} | RAM: {fmt_gb(r['host_ram'])}")
    logging.info(f"Target CPU: {r['target_cpu']} | RAM: {fmt_gb(r['target_ram'])}")
    
    libs_pct = r['data'].get("overall", {}).get("relative", {}).get("libs_pct", {})
    if libs_pct:
        def fmt_lib(v):
            if v is None:
                return "N/A"
            return f"{'+' if v >= 0 else ''}{v:.2f}% ({'host faster' if v > 0 else 'host slower' if v < 0 else 'same'})"
        
        logging.info(f"  Pandas diff: {fmt_lib(libs_pct.get('pandas'))}")
        logging.info(f"  Polars diff: {fmt_lib(libs_pct.get('polars'))}")
        logging.info(f"  DuckDB diff: {fmt_lib(libs_pct.get('duckdb'))}")
        if 'fireducks' in libs_pct:
            logging.info(f"  FireDucks diff: {fmt_lib(libs_pct.get('fireducks'))}")

if errors:
    logging.error(f"{len(errors)} comparison(s) failed.")
    sys.exit(1)