import json
import logging

logging.basicConfig(level=logging.INFO)

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
        logging.error(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON format in file: {file_path}")
        return None

def extract_os_memory_info(data: dict) -> dict:
    """
    Extract OS and memory filter information from the given data.

    :param data: Dictionary containing JSON data.
    :return: Dictionary with extracted OS intersection and memory filter information.
    """
    os_f = data.get('os_intersection_filter', {})
    mem_f = data.get('memory_filter', {})
    return {
        'host_a_os': os_f.get('host_a_os'),
        'host_b_os': os_f.get('host_b_os'),
        'common_os': os_f.get('common_os'),
        'excluded_os': os_f.get('excluded_os'),
        'os_applied': os_f.get('applied', 'N/A'),
        'os_reason': os_f.get('reason', 'N/A'),
        'mem_applied': mem_f.get('applied', 'N/A'),
        'mem_warning': mem_f.get('warning', 'None')
    }

def extract_by_os_info(data: dict) -> list:
    """
    Extract information by OS from the given data.

    :param data: Dictionary containing JSON data.
    :return: List of dictionaries with OS-specific information.
    """
    return [
        {
            'os_name': os_blk['os'],
            'rows_a': os_blk['summary_a'].get('rows', 0),
            'rows_b': os_blk['summary_b'].get('rows', 0),
            'mean_a': os_blk['summary_a'].get('overall_mean', 0.0),
            'mean_b': os_blk['summary_b'].get('overall_mean', 0.0)
        }
        for os_blk in data.get('by_os', [])
    ]

def print_data(pair: str, os_info: dict, by_os_info: list):
    """
    Print the OS and memory information.

    :param pair: String indicating the comparison pair.
    :param os_info: Dictionary containing OS filter and intersection information.
    :param by_os_info: List of dictionaries with OS-specific information.
    """
    logging.info(f'=== {pair} ===')
    logging.info(f'  host_a OS: {os_info["host_a_os"]}')
    logging.info(f'  host_b OS: {os_info["host_b_os"]}')
    logging.info(f'  common OS: {os_info["common_os"]}')
    logging.info(f'  excluded OS: {os_info["excluded_os"]}')
    logging.info(f'  OS filter applied: {os_info["os_applied"]}')
    logging.info(f'  OS reason: {os_info["os_reason"]}')
    logging.info(f'  Memory filter: {os_info["mem_applied"]}')
    logging.info(f'  Memory warning: {os_info["mem_warning"]}')
    
    for os_blk in by_os_info:
        logging.info(f'  {os_blk["os_name"]}: rows_a={os_blk["rows_a"]}, rows_b={os_blk["rows_b"]}, '
                      f'mean_a={os_blk["mean_a"]:.3f}, mean_b={os_blk["mean_b"]:.3f}')

def main():
    pairs = ['compare_ZBookPowerG9_vs_HP-ZB-Power-G10', 'compare_ZBookPowerG9_vs_HP-ZB-Fury-G10']
    
    for pair in pairs:
        path = f'data/custom_results/{pair}.json'
        data = read_json_file(path)
        
        if not data:
            continue
        
        os_info = extract_os_memory_info(data)
        by_os_info = extract_by_os_info(data)
        
        print_data(pair, os_info, by_os_info)

if __name__ == "__main__":
    main()
