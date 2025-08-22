# scripts/log_generator.py
import json
import random
import numpy as np
import datetime as dt
import ipaddress
import time

def generate_log_entry(source_network: str = "192.168.1.0/24", destination_network: str = "10.1.26.0/24", time_in_weeks: int = 12) -> dict:
    """
    Generates a random log entry with a timestamp, source IP, destination IP, port, protocol, event, severity, and action.

    Returns:
        dict: A dictionary containing the generated log entry.
    """
    # ip_range = range(2, 255)
    ports = [22, 80, 443, 3389, 53, 110, 143, 993, 995, 8080, 8443, 21, 20, 137, 138, 139, 445, 161, 162, 123, 5060, 5061, 8000, 8001]
    protocols = ["TCP", "UDP"]
    WEEKS_AGO = time_in_weeks
    events = [
        {"event": "Failed SSH login", "severity": "warning", "actions": ["ignored", "blocked"]},
        {"event": "RDP brute force attempt", "severity": "critical", "actions": ["blocked", "reported"]},
        {"event": "Suspicious DNS activity", "severity": "info", "actions": ["monitored", "allowed"]},
        {"event": "Failed SSH login", "severity": "warning", "actions": ["ignored", "blocked"]},
        {"event": "RDP brute force attempt", "severity": "critical", "actions": ["blocked", "reported"]},
        {"event": "Suspicious DNS activity", "severity": "info", "actions": ["monitored", "allowed"]},
        {"event": "Port scan detected", "severity": "warning", "actions": ["blocked", "rate limited"]},
        {"event": "Malware signature match", "severity": "critical", "actions": ["quarantined", "eradicated"]},
        {"event": "Suspicious file activity", "severity": "critical", "actions": ["blocked", "eradicated"]},
        {"event": "Unusual login location", "severity": "warning", "actions": ["monitored", "verified"]},
        {"event": "Failed login attempts", "severity": "warning", "actions": ["blocked", "rate limited"]},
        {"event": "Potential SQL injection attempt", "severity": "critical", "actions": ["blocked", "reported"]},
        {"event": "Cross-site scripting (XSS) attempt", "severity": "critical", "actions": ["blocked", "reported"]},
        {"event": "System file modification", "severity": "warning", "actions": ["monitored", "verified"]},
        {"event": "Malware outbreak detected", "severity": "critical", "actions": ["quarantined", "eradicated"]},
        {"event": "Denial of Service (DoS) attack detected", "severity": "critical", "actions": ["blocked", "reported"]},
        {"event": "Potential data exfiltration attempt", "severity": "critical", "actions": ["blocked", "reported"]},
        {"event": "Suspicious network traffic", "severity": "warning", "actions": ["monitored", "scanned"]}
    ]

    event = random.choice(events)
    action = random.choice(event["actions"])

    # Make the severity more dynamic
    if event["event"] == "RDP brute force attempt" and random.random() < 0.5:
        event["severity"] = "high"
    
    # Generate random timestamp starting x weeks ago
    start_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(weeks=WEEKS_AGO)

    # Using random(might be slower)
    random_time = start_time + dt.timedelta(seconds=random.randint(0, int(dt.timedelta(weeks=WEEKS_AGO).total_seconds())))
    timestamp = random_time.isoformat(timespec='seconds').replace("+00:00", "Z") # Zulu time(UTC)

    # Using numpy(expected to be faster when the data grows)
    #random_ts = np.random.uniform(start_time.timestamp(), dt.datetime.now(dt.timezone.utc).timestamp())
    #timestamp = dt.datetime.fromtimestamp(random_ts, tz=dt.timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z")

    # Generate random IP address from 'source network' range
    attacker_network = ipaddress.ip_network(source_network, strict=False)
    source_ip = str(ipaddress.ip_address(random.randint(int(attacker_network.network_address), int(attacker_network.broadcast_address))))

    # Generate random IP address from 'destination network' range
    corp_network = ipaddress.ip_network(destination_network, strict=False)
    destination_ip = str(ipaddress.ip_address(random.randint(int(corp_network.network_address), int(corp_network.broadcast_address))))
    return {
        "timestamp": timestamp,
        "source_ip": source_ip,
        "destination_ip": destination_ip,
        "port": random.choice(ports),
        "protocol": random.choice(protocols),
        "event": event["event"],
        "severity": event["severity"],
        "action": action
    }
def generate_logs(n: int = 100_000, source_network: str = "192.168.1.0/24", destination_network: str = "10.1.26.0/24", time_in_weeks: int = 12, output_file: str = "data/generated_logs_v1.json") -> None:
    """
    Generates a specified number of log entries and writes them to a JSON file.

    Args:
        n (int): The number of log entries to generate. Defaults to 1000.
        output_file (str): The path to the JSON file where the logs will be written. Defaults to "data/generated_logs.json".

    Raises:
        ValueError: If n is not a positive integer or if output_file does not end with .json.

    Returns:
        None
    """
    if not isinstance(n, int) or n <= 0:
        raise ValueError("n must be a positive integer")
    if not isinstance(output_file, str) or not output_file.endswith(".json"):
        raise ValueError("output_file must be a string ending with .json")

    try:
        logs = [generate_log_entry(source_network, destination_network, time_in_weeks) for _ in range(n)] # Generate the log entries(list comprehension)
        logs = [log for log in logs if log is not None]  # Filter out any None log entries
        with open(output_file, "w") as f:
            json.dump(logs, f, indent=2)
    except Exception as e:
        print(f"An error occurred while generating logs: {e}")

def main():
    """
    Generates 100,000 log entries and writes them to data/generated_logs_v1.json,
    printing the time taken to do so.

    Returns:
        None
    """
    start_time = time.time()
    generate_logs(n=1_000_000)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()