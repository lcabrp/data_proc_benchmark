import json

for pair in ['compare_ZBookPowerG9_vs_HP-ZB-Power-G10', 'compare_ZBookPowerG9_vs_HP-ZB-Fury-G10']:
    path = f'data/custom_results/{pair}.json'
    with open(path, encoding='utf-8') as f:
        d = json.load(f)
    osf = d.get('os_intersection_filter', {})
    mem = d.get('memory_filter', {})
    print(f'=== {pair} ===')
    print(f'  host_a OS: {osf.get("host_a_os")}')
    print(f'  host_b OS: {osf.get("host_b_os")}')
    print(f'  common OS: {osf.get("common_os")}')
    print(f'  excluded OS: {osf.get("excluded_os")}')
    print(f'  OS filter applied: {osf.get("applied")}')
    print(f'  OS reason: {osf.get("reason")}')
    print(f'  Memory filter: {mem.get("applied", "N/A")}')
    print(f'  Memory warning: {mem.get("warning", "None")}')
    by_os = d.get('by_os', [])
    for os_blk in by_os:
        os_name = os_blk['os']
        sa = os_blk['summary_a']
        sb = os_blk['summary_b']
        print(f'  {os_name}: rows_a={sa.get("rows")}, rows_b={sb.get("rows")}, '
              f'mean_a={sa.get("overall_mean",0):.3f}, mean_b={sb.get("overall_mean",0):.3f}')
    print()
