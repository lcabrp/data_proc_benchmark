import json

path = 'data/custom_results/compare_ZBookPowerG9_vs_HP-ZB-Fury-G10.json'
with open(path, encoding='utf-8') as f:
    d = json.load(f)

osf = d.get('os_intersection_filter', {})
mem = d.get('memory_filter', {})
outliers = d.get('outliers_removed', {})

print("=== OS Intersection Filter ===")
print(f"  common OS: {osf.get('common_os')}")
print(f"  excluded OS: {osf.get('excluded_os')}")

print("\n=== Memory Filter ===")
for k, v in mem.items():
    print(f"  {k}: {v}")

print("\n=== Outliers Removed ===")
for k, v in outliers.items():
    print(f"  {k}: {v}")

print("\n=== Overall ===")
o = d['overall']
sa = o['summary_a']
sb = o['summary_b']
print(f"  ZBookPowerG9: rows={sa['rows']}, mean={sa['overall_mean']:.3f}, mem={sa['mem_total_mean']:.1f}GB")
print(f"  HP-ZB-Fury-G10: rows={sb['rows']}, mean={sb['overall_mean']:.3f}, mem={sb['mem_total_mean']:.1f}GB")
print(f"  relative pct: {o['relative']['overall_mean_pct']:.2f}%")

print("\n=== Per-OS Breakdown ===")
for os_blk in d.get('by_os', []):
    os_name = os_blk['os']
    sa_os = os_blk['summary_a']
    sb_os = os_blk['summary_b']
    pct = os_blk['relative']['overall_mean_pct']
    print(f"  {os_name}:")
    print(f"    ZBookPowerG9: rows={sa_os['rows']}, mean={sa_os['overall_mean']:.3f}, mem_total={sa_os.get('mem_total_mean',0):.1f}GB, mem_avail={sa_os.get('mem_avail_mean',0):.1f}GB")
    print(f"    HP-ZB-Fury-G10: rows={sb_os['rows']}, mean={sb_os['overall_mean']:.3f}, mem_total={sb_os.get('mem_total_mean',0):.1f}GB, mem_avail={sb_os.get('mem_avail_mean',0):.1f}GB")
    print(f"    pct: {pct:.2f}%")
