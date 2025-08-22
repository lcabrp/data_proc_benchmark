# scripts/log_generator.py
# This module is similar to v1, but it uses OOP

import json
import random
import datetime as dt
import ipaddress
import time

# This class is used to generate a single log entry

class NetworkLogEntry:
    def __init__(self, source_network: str = "192.168.1.0/24", destination_network: str = "10.1.26.0/24", time_in_weeks: int = 12) -> None:
        self.attacker_network = ipaddress.ip_network(source_network, strict=False)
        self.corp_network = ipaddress.ip_network(destination_network, strict=False)
        self.WEEKS_AGO = time_in_weeks
        self.start_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(weeks=self.WEEKS_AGO)
        self.end_time = dt.datetime.now(dt.timezone.utc)
        self.PORT_METADATA = {
            22:  {"service": "SSH", "threat_level": "high"},
            23:  {"service": "Telnet", "threat_level": "high"},
            80:  {"service": "HTTP", "threat_level": "low"},
            443: {"service": "HTTPS", "threat_level": "low"},
            3389:{"service": "RDP", "threat_level": "very_high"},
            53:  {"service": "DNS", "threat_level": "medium"},
            110: {"service": "POP3", "threat_level": "medium"},
            143: {"service": "IMAP", "threat_level": "medium"},
            993: {"service": "IMAPS", "threat_level": "low"},
            995: {"service": "POP3S", "threat_level": "low"},
            8080:{"service": "Alt HTTP", "threat_level": "medium"},
            8443:{"service": "Alt HTTPS", "threat_level": "low"},
            20:  {"service": "FTP Data", "threat_level": "medium"},
            21:  {"service": "FTP Control", "threat_level": "medium"},
            137: {"service": "NetBIOS Name", "threat_level": "high"},
            138: {"service": "NetBIOS Datagram", "threat_level": "high"},
            139: {"service": "NetBIOS Session", "threat_level": "high"},
            445: {"service": "SMB", "threat_level": "very_high"},
            161: {"service": "SNMP", "threat_level": "medium"},
            162: {"service": "SNMP Trap", "threat_level": "medium"},
            123: {"service": "NTP", "threat_level": "low"},
            465: {"service": "SMTPS", "threat_level": "low"},
            587: {"service": "SMTP STARTTLS", "threat_level": "low"},
            5060:{"service": "SIP", "threat_level": "medium"},
            5061:{"service": "SIPS", "threat_level": "low"},
            8000:{"service": "Custom Web", "threat_level": "medium"},
            8001:{"service": "Custom Web", "threat_level": "medium"},
            4444:{"service": "Metasploit", "threat_level": "critical"},
        }

        self.ports = list(self.PORT_METADATA.keys())
        self.protocols = ["TCP", "UDP"]
        self.events = [
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

        weights = {"low": 1, "medium": 2, "high": 3, "very_high": 4, "critical": 5}
        self.port_weights = [weights[self.PORT_METADATA[p]["threat_level"]] for p in self.ports]

    def weighted_port_choice(self):
        return random.choices(self.ports, weights=self.port_weights, k=1)[0]
    
    def generate_log_entry(self) -> dict:
        """
        Generates a random log entry with a timestamp, source IP, destination IP, port, protocol, event, severity, and action.

        Returns:
            dict: A dictionary containing the generated log entry.
        """
        event = random.choice(self.events)
        action = random.choice(event["actions"])

        # Make the severity more dynamic
        if event["event"] == "RDP brute force attempt" and random.random() < 0.5:
            event["severity"] = "high"
        
        # Generate random timestamp starting x weeks ago

        # Using random(might be slower)
        random_time = self.start_time + dt.timedelta(seconds=random.randint(0, int(dt.timedelta(weeks=self.WEEKS_AGO).total_seconds())))
        timestamp = random_time.isoformat(timespec='seconds').replace("+00:00", "Z") # Zulu time(UTC)

        # Using numpy(expected to be faster when the data grows)
        #random_ts = np.random.uniform(start_time.timestamp(), dt.datetime.now(dt.timezone.utc).timestamp())
        #timestamp = dt.datetime.fromtimestamp(random_ts, tz=dt.timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z")

        # Generate random IP address from 'source network' range
        
        source_ip = str(ipaddress.ip_address(random.randint(int(self.attacker_network.network_address), int(self.attacker_network.broadcast_address))))

        # Generate random IP address from 'destination network' range
        destination_ip = str(ipaddress.ip_address(random.randint(int(self.corp_network.network_address), int(self.corp_network.broadcast_address))))
        return {
            "timestamp": timestamp,
            "source_ip": source_ip,
            "destination_ip": destination_ip,
            "port": self.weighted_port_choice(),
            "protocol": random.choice(self.protocols),
            "event": event["event"],
            "severity": event["severity"],
            "action": action
        }
    def generate_logs_json(self, n: int = 100_000, output_file: str = "data/generated_logs_v11.json") -> None:
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
            logs = [self.generate_log_entry() for _ in range(n)] # Generate the log entries(list comprehension)
            logs = [log for log in logs if log is not None]  # Filter out any None log entries
            with open(output_file, "w") as f:
                json.dump(logs, f, indent=2)
        except Exception as e:
            print(f"An error occurred while generating logs: {e}")

    def generate_logs_ndjson(self, n: int = 100_000, output_file: str = "data/generated_logs_v11.ndjson") -> None:
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
        
        if not isinstance(output_file, str) or not output_file.endswith(".ndjson"):
            raise ValueError("output_file must be a string ending with .ndjson")

        try:
            with open(output_file, "w") as f:
                for _ in range(n):
                    log = self.generate_log_entry()
                    if log is not None:
                        json.dump(log, f)
                        f.write('\n')
        except Exception as e:
            print(f"An error occurred while generating logs: {e}")

def main():
    """
    Generates 100,000 log entries and writes them to data/generated_logs_v11.json,
    printing the time taken to do so.

    Returns:
        None
    """
    start_time = time.time()
    nle = NetworkLogEntry()
    nle.generate_logs_json(n=1_000_000)
    end_time = time.time()
    print(f"Time taken to create 1M logs using json format: {end_time - start_time:.2f} seconds")

    start_time = time.time()
    nle.generate_logs_ndjson(n=1_000_000)
    end_time = time.time()
    print(f"Time taken to create 1M logs  using ndjson format: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
