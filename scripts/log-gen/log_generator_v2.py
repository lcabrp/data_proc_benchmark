import json
import numpy as np
import datetime as dt
import ipaddress
from itertools import islice

def generate_bulk_logs(n: int = 100_000, time_in_weeks: int = 12, output_file: str = "data/generated_logs_v2.json"):
    # --- Predefine static lists ---
    ports = np.array([22, 80, 443, 3389, 53, 110, 143, 993, 995, 8080, 8443, 21, 20, 137, 138, 139, 445, 161, 162,
                      123, 5060, 5061, 8000, 8001])
    protocols = np.array(["TCP", "UDP"])
    events = [
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
    event_indices = np.random.randint(0, len(events), n) # numpy.ndarray of integers
    chosen_events = [events[i] for i in event_indices] # list of dictionaries
    event_names = [e['event'] for e in chosen_events] # list of strings
    severities = [e['severity'] for e in chosen_events] # list of strings
    actions_list = [e['actions'] for e in chosen_events] # list of lists
    actions = [np.random.choice(a) for a in actions_list] # list of strings

    # Dynamic severity adjustment vectorized for "RDP brute force attempt"
    severities = [
        "high" if event_names[i] == "RDP brute force attempt" and np.random.rand() < 0.5 else severities[i]
        for i in range(n)
    ]

    # --- Generate random times, ports, protocols, IPs ---
    start_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(weeks=time_in_weeks)
    now_time = dt.datetime.now(dt.timezone.utc)
    timestamps = np.random.uniform(start_time.timestamp(), now_time.timestamp(), size=n)
    iso_timestamps = [
        dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')
        for ts in timestamps
    ]
    # source_ip: "192.168.1.X"
    source_ip_last_octet = np.random.randint(2, 255, size=n)
    source_ips = [f"192.168.1.{x}" for x in source_ip_last_octet]

    # destination_ip in 10.1.26.0/24
    net = ipaddress.ip_network('10.1.26.0/24', strict=False)
    dest_ip_ints = np.random.randint(int(net.network_address), int(net.broadcast_address)+1, size=n)
    dest_ips = [str(ipaddress.ip_address(int(x))) for x in dest_ip_ints]

    # ports, protocols
    ports_selected = np.random.choice(ports, size=n)
    protocols_selected = np.random.choice(protocols, size=n)
    # --- Assemble the logs ---
    # Created a list of dictionaries using list comprehension
    logs = [
        {
            "timestamp": iso_timestamps[i],
            "source_ip": source_ips[i],
            "destination_ip": dest_ips[i],
            "port": int(ports_selected[i]),
            "protocol": str(protocols_selected[i]),
            "event": event_names[i],
            "severity": severities[i],
            "action": actions[i],
        }
        for i in range(n)
    ]

    with open(output_file, "w") as f:
        json.dump(logs, f, indent=2)

if __name__ == "__main__":
    import time
    start = time.time()
    generate_bulk_logs(n=1_000_000)
    end = time.time()
    print(f"Time taken: {end-start:.2f} seconds")