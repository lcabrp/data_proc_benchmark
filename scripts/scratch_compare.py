import subprocess
import json
import sys
import os

target = "IdeaPadPro5i-2"
compare_to = [
    "Legion7-16IRX9",
    "IdeaPadPro5i"
]

results = []

for host in compare_to:
    json_path = f"{host}_vs_{target}.json"
    cmd = [
        sys.executable,
        "scripts/tools/compare_hosts.py",
        "--csv", "data/benchmark_results.csv",
        "--host", target,
        "--host", host,
        "--json-out", json_path,
        "--force"
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
        
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        overall = data.get("overall", {})
        sum_a = overall.get("summary_a", {})
        sum_b = overall.get("summary_b", {})
        rel = overall.get("relative", {})
        
        # In JSON, host_a is always the target (first --host argument) and host_b is the compared host.
        # Wait, the script passes --host target --host host.
        # Let's check host_a and host_b in data
        host_a_name = data.get("host_a")
        host_b_name = data.get("host_b")
        
        target_sum = sum_a if host_a_name == target else sum_b
        host_sum = sum_b if host_a_name == target else sum_a
        
        pct = rel.get("overall_mean_pct", 0)
        
        t_mean = target_sum.get("overall_mean", 0)
        h_mean = host_sum.get("overall_mean", 0)
        
        tie_thresh = data.get("tie_threshold_pct", 5.0)
        if pct < tie_thresh:
            winner = "Tie"
        elif t_mean < h_mean:
            winner = target
        else:
            winner = host
            
        results.append({
            "host": host,
            "host_cpu": host_sum.get("cpu_brand", "Unknown"),
            "host_ram": host_sum.get("mem_total_mean", 0),
            "target_cpu": target_sum.get("cpu_brand", "Unknown"),
            "target_ram": target_sum.get("mem_total_mean", 0),
            "winner": winner,
            "percent_diff": pct,
            "data": data
        })
    except subprocess.CalledProcessError as e:
        print(f"Error comparing {host}: {e.stderr.decode('utf-8', errors='replace')}")

for r in results:
    print(f"--- {r['host']} vs {target} ---")
    diff_str = f"{r['percent_diff']:.2f}%" if r['percent_diff'] is not None else "N/A"
    print(f"Host CPU: {r['host_cpu']} | RAM: {r['host_ram']:.1f}GB")
    print(f"Target CPU: {r['target_cpu']} | RAM: {r['target_ram']:.1f}GB")
    print(f"Winner: {r['winner']} by {diff_str}")
    
    libs_pct = r['data'].get("overall", {}).get("relative", {}).get("libs_pct", {})
    if libs_pct:
        print(f"  Pandas diff: {libs_pct.get('pandas', 0):.2f}%")
        print(f"  Polars diff: {libs_pct.get('polars', 0):.2f}%")
        print(f"  DuckDB diff: {libs_pct.get('duckdb', 0):.2f}%")
    
    print("")
