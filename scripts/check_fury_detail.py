import json

def read_json_file(file_path: str) -> dict:
    """
    Read a JSON file from the specified path.

    :param file_path: Absolute path to the target JSON file.
    :return: Parsed JSON data as a dictionary, or None if an error occurs.
    """
    try:
        with open(file_path, encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Invalid JSON format in file: {file_path}")
        return None

def print_os_filter_info(os_filter_info: dict):
    """
    Print OS filter information.

    :param os_filter_info: Dictionary containing OS intersection and memory filter information.
    """
    print("=== OS Intersection Filter ===")
    print(f"  common OS: {os_filter_info.get('common_os')}")
    print(f"  excluded OS: {os_filter_info.get('excluded_os')}")

def print_memory_filter_info(memory_filter_info: dict):
    """
    Print memory filter information.

    :param memory_filter_info: Dictionary containing memory filter details.
    """
    print("\n=== Memory Filter ===")
    for k, v in memory_filter_info.items():
        print(f"  {k}: {v}")

def print_outliers_removed_info(outliers_removed_info: dict):
    """
    Print outliers removed information.

    :param outliers_removed_info: Dictionary containing outliers removal details.
    """
    print("\n=== Outliers Removed ===")
    for k, v in outliers_removed_info.items():
        print(f"  {k}: {v}")

def print_overall_statistics(results_data: dict):
    """
    Print overall statistics.

    :param results_data: Dictionary containing the main results data.
    """
    o = results_data['overall']
    sa = o['summary_a']
    sb = o['summary_b']
    print("\n=== Overall ===")
    print(f"  ZBookPowerG9: rows={sa['rows']}, mean={sa['overall_mean']:.3f}, mem_total={sa['mem_total_mean']:.1f}GB")
    print(f"  HP-ZB-Fury-G10: rows={sb['rows']}, mean={sb['overall_mean']:.3f}, mem_total={sb['mem_total_mean']:.1f}GB")
    print(f"  relative pct: {o['relative']['overall_mean_pct']:.2f}%")

def print_per_os_breakdown(results_data: dict):
    """
    Print per-OS breakdown.

    :param results_data: Dictionary containing the main results data.
    """
    print("\n=== Per-OS Breakdown ===")
    for os_blk in results_data.get('by_os', []):
        os_name = os_blk['os']
        sa_os = os_blk['summary_a']
        sb_os = os_blk['summary_b']
        pct = os_blk['relative']['overall_mean_pct']
        print(f"  {os_name}:")
        print(f"    ZBookPowerG9: rows={sa_os['rows']}, mean={sa_os['overall_mean']:.3f}, mem_total={sa_os.get('mem_total_mean',0):.1f}GB, mem_avail={sa_os.get('mem_avail_mean',0):.1f}GB")
        print(f"    HP-ZB-Fury-G10: rows={sb_os['rows']}, mean={sb_os['overall_mean']:.3f}, mem_total={sb_os.get('mem_total_mean',0):.1f}GB, mem_avail={sb_os.get('mem_avail_mean',0):.1f}GB")
        print(f"    pct: {pct:.2f}%")

def main():
    path = 'data/custom_results/compare_ZBookPowerG9_vs_HP-ZB-Fury-G10.json'
    data = read_json_file(path)
    
    if not data:
        return
    
    os_filter_info = {'common_os': data.get('os_intersection_filter', {}).get('common_os'),
                       'excluded_os': data.get('os_intersection_filter', {}).get('excluded_os')}
    
    memory_filter_info = data.get('memory_filter', {})
    outliers_removed_info = data.get('outliers_removed', {})

    print_os_filter_info(os_filter_info)
    print_memory_filter_info(memory_filter_info)
    print_outliers_removed_info(outliers_removed_info)
    print_overall_statistics(data)
    print_per_os_breakdown(data)

if __name__ == "__main__":
    main()
