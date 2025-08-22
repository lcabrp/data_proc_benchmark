import random
import json
import itertools
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional


class SyntheticSIEMLogGenerator:
    """
    Generates realistic synthetic SIEM log events in NDJSON format.

    Features:
    - Simulates active and passive hosts (skewed event distribution)
    - Adds variable event rates on weekdays/weekends
    - Simulates attack campaigns (bursts of suspicious activity)
    - Produces varied event types with optional metadata
    """

    EVENT_TYPES = [
        "auth_success", "auth_fail", "malware_detected", "port_scan", "firewall_block",
        "file_access", "usb_insert", "policy_violation", "process_start", "config_change"
    ]
    EVENT_PROBS = [
        0.35, 0.22, 0.03, 0.05, 0.06, 0.12, 0.03, 0.04, 0.08, 0.02
    ]

    USERS = [f"user{i}" for i in range(1, 41)]  # 40 users for realism

    def __init__(
        self,
        network_prefix: str = "192.168.1.",
        n_hosts: int = 80,
        n_active_hosts: int = 15,
        days: int = 180,
        events_per_day: int = 5500,
        campaign_count: int = 5,
        campaign_burst_length: Tuple[int, int] = (1, 3),
        campaign_intensity: Tuple[int, int] = (100, 400),
        seed: Optional[int] = 42
    ):
        """
        Initialize the generator.
        Args:
            network_prefix (str): Network IP prefix for hosts.
            n_hosts (int): Number of hosts (1-254).
            n_active_hosts (int): Number of active hosts with high event volume.
            days (int): Number of days in simulation.
            events_per_day (int): Avg. total events per day (normal + campaign).
            campaign_count (int): Number of attack campaigns to simulate.
            campaign_burst_length (tuple): Min/max days a campaign lasts.
            campaign_intensity (tuple): Min/max total extra events per campaign/day.
            seed (Optional[int]): Seed for reproducibility.
        """
        if seed is not None:
            random.seed(seed)
        self.network_prefix = network_prefix
        self.n_hosts = min(n_hosts, 254)
        self.n_active_hosts = min(n_active_hosts, self.n_hosts)
        self.days = days
        self.events_per_day = events_per_day
        self.campaign_count = campaign_count
        self.campaign_burst_length = campaign_burst_length
        self.campaign_intensity = campaign_intensity
        self.hosts = self._generate_hosts()
        self.active_hosts = set(random.sample(self.hosts, self.n_active_hosts))
        self.passive_hosts = set(self.hosts) - self.active_hosts
        self.first_day = datetime.now() - timedelta(days=self.days)

    def _generate_hosts(self) -> List[str]:
        """Generates host IPs in the /24 subnet."""
        return [f"{self.network_prefix}{i}" for i in range(1, self.n_hosts + 1)]

    def _choose_user(self, host_ip: str) -> str:
        """Assign a user, skewed for realism."""
        # Frequent users for active hosts, random otherwise
        if host_ip in self.active_hosts:
            return random.choice(self.USERS[:10])
        return random.choice(self.USERS)

    def _generate_campaigns(self) -> List[Dict]:
        """Randomly generate time windows and targets for attack 'campaigns'."""
        campaigns = []
        day_range = range(self.days)
        for _ in range(self.campaign_count):
            start = random.choice(day_range[:-self.campaign_burst_length[1]])
            burst_days = random.randint(*self.campaign_burst_length)
            burst_hosts = random.sample(self.hosts, k=random.randint(2, min(9, self.n_hosts // 6)))
            event_type = random.choices(
                ["malware_detected", "auth_fail", "port_scan"], weights=[0.5, 0.3, 0.2]
            )[0]
            campaigns.append({
                "start_day": start,
                "length": burst_days,
                "hosts": burst_hosts,
                "event_type": event_type,
                "intensity": random.randint(*self.campaign_intensity)
            })
        return campaigns

    def _event_details(self, event_type: str) -> Dict:
        """Produce extra fields for specific event types."""
        details = {}
        if event_type == "auth_fail":
            details["fail_reason"] = random.choice(["bad_password", "user_locked", "expired"])
        elif event_type == "malware_detected":
            details["malware_name"] = random.choice(["Emotet", "TrickBot", "AgentTesla", "unknown"])
            details["severity"] = random.choice(["high", "medium", "low"])
        elif event_type == "port_scan":
            details["proto"] = random.choice(["tcp", "udp"])
            details["target_port"] = random.choice([22, 80, 135, 443, 445, 3389])
        elif event_type == "file_access":
            details["file"] = random.choice(["/etc/passwd", "C:\\Windows\\secret.txt", "/var/log/syslog"])
            details["access_type"] = random.choice(["read", "write", "delete"])
        elif event_type == "firewall_block":
            details["proto"] = random.choice(["tcp", "udp"])
            details["rule"] = random.choice(["block-list", "IDS", "manual"])
        return details

    def _risk_level(self, event_type: str) -> str:
        """Assign a risk score for event type."""
        mapping = {
            "auth_success": "low",
            "auth_fail": "medium",
            "malware_detected": "high",
            "port_scan": "medium",
            "firewall_block": "medium",
            "file_access": "low",
            "usb_insert": "medium",
            "policy_violation": "medium",
            "process_start": "low",
            "config_change": "medium"
        }
        return mapping.get(event_type, "medium")

    def _random_time_on_day(self, day: int) -> datetime:
        """Return a random time within a specific day offset."""
        base = self.first_day + timedelta(days=day)
        seconds = random.randint(0, 86399)
        return base + timedelta(seconds=seconds)

    def _is_weekend(self, day: int) -> bool:
        """Return True if the simulated day is a weekend."""
        date = self.first_day + timedelta(days=day)
        return date.weekday() >= 5

    def generate(self, output_file: str) -> None:
        """
        Generate and write NDJSON logs to the given file.
        Args:
            output_file (str): Path to the output NDJSON log file.
        """
        try:
            campaigns = self._generate_campaigns()
            campaigns_by_day = {d: [] for d in range(self.days)}
            for c in campaigns:
                for i in range(c["length"]):
                    d = c["start_day"] + i
                    if d < self.days:
                        campaigns_by_day[d].append(c)

            with open(output_file, "w") as f:
                for day in range(self.days):
                    # Determine weekday/weekend adjustment
                    current_events = self.events_per_day
                    if self._is_weekend(day):
                        current_events = int(current_events * random.uniform(0.5, 0.7))  # Fewer events
                    # Active hosts get 70-80% of events, passive spread remainder
                    n_active = int(current_events * random.uniform(0.70, 0.80))
                    n_passive = current_events - n_active
                    host_event_tuples = (
                        random.choices(list(self.active_hosts), k=n_active)
                        + random.choices(list(self.passive_hosts), k=n_passive)
                    )

                    # Normal (background) events
                    for host_ip in host_event_tuples:
                        try:
                            event_type = random.choices(
                                self.EVENT_TYPES, weights=self.EVENT_PROBS, k=1
                            )[0]
                            event_time = self._random_time_on_day(day)
                            event = {
                                "ts": event_time.isoformat(timespec="seconds"),
                                "host": host_ip,
                                "event_type": event_type,
                                "user": self._choose_user(host_ip),
                                "risk": self._risk_level(event_type),
                                **self._event_details(event_type)
                            }
                            f.write(json.dumps(event) + "\n")
                        except Exception as ex:
                            # Defensive: continue, but print the problem
                            print(f"Error creating event for host {host_ip}: {ex}")

                    # Campaigns (add extra "anomalous" events)
                    for campaign in campaigns_by_day[day]:
                        for _ in range(campaign["intensity"] // campaign["length"]):
                            host_ip = random.choice(campaign["hosts"])
                            event_time = self._random_time_on_day(day)
                            event = {
                                "ts": event_time.isoformat(timespec="seconds"),
                                "host": host_ip,
                                "event_type": campaign["event_type"],
                                "user": self._choose_user(host_ip),
                                "risk": self._risk_level(campaign["event_type"]),
                                "campaign_id": f"camp_{campaigns.index(campaign) + 1}",
                                **self._event_details(campaign["event_type"])
                            }
                            f.write(json.dumps(event) + "\n")
        except Exception as main_ex:
            print(f"Fatal error during log generation: {main_ex}")
            raise

if __name__ == "__main__":
    # Quick usage example (tweak arguments as needed)
    generator = SyntheticSIEMLogGenerator(
        network_prefix="10.1.0.",       # Your subnet base
        n_hosts=80,                     # Number of hosts in your simulated network
        n_active_hosts=15,              # "Noisy" (high-activity) hosts
        days=180,                       # 6 months
        events_per_day=5800,            # Target daily event count (before campaign bursts)
        campaign_count=7,               # Number of attack campaigns to plant
    )
    import time
    start = time.time()
    generator.generate("data/synthetic_logs.ndjson")
    end = time.time()
    print(f"Time taken: {end-start:.2f} seconds")
    print("Synthetic NDJSON log generation completed!")