import pandas as pd
import numpy as np
import datetime as dt
from typing import List

def generate_logs(n=1_000_000):
    """Generate realistic synthetic log data with rich fields for benchmarking"""
    np.random.seed(42)
    
    # Time range: last 30 days
    start_time = dt.datetime.now() - dt.timedelta(days=30)
    timestamps = pd.date_range(start=start_time, periods=n, freq='30s')
    
    # Network data
    source_ips = np.random.choice([
        "192.168.1.10", "192.168.1.15", "192.168.1.20", "192.168.1.25",
        "10.0.0.5", "10.0.0.8", "10.0.0.12", "172.16.0.3", "172.16.0.7"
    ], size=n, p=[0.15, 0.12, 0.10, 0.08, 0.15, 0.12, 0.08, 0.10, 0.10])
    
    dest_ips = np.random.choice([
        "203.0.113.1", "198.51.100.5", "93.184.216.34", "8.8.8.8",
        "1.1.1.1", "208.67.222.222", "185.199.108.153"
    ], size=n)
    
    # Ports with realistic distribution
    ports = np.random.choice([80, 443, 22, 21, 25, 53, 3389, 8080, 8443], 
                           size=n, p=[0.35, 0.30, 0.10, 0.05, 0.05, 0.05, 0.03, 0.04, 0.03])
    
    protocols = np.random.choice(["TCP", "UDP", "ICMP"], size=n, p=[0.7, 0.25, 0.05])
    
    # Security events
    event_types = np.random.choice([
        "login_success", "login_failed", "file_access", "network_scan", 
        "malware_detected", "suspicious_activity", "data_transfer", "system_error"
    ], size=n, p=[0.25, 0.15, 0.20, 0.08, 0.05, 0.07, 0.15, 0.05])
    
    severities = np.random.choice(["low", "medium", "high", "critical"], 
                                size=n, p=[0.5, 0.3, 0.15, 0.05])
    
    # User data
    users = np.random.choice([
        "alice.smith", "bob.jones", "charlie.brown", "diana.wilson", 
        "eve.davis", "frank.miller", "grace.taylor", "system", "admin"
    ], size=n, p=[0.15, 0.12, 0.10, 0.08, 0.10, 0.08, 0.07, 0.15, 0.15])
    
    # Status codes and bytes
    status_codes = np.random.choice([200, 404, 500, 403, 401, 302], 
                                  size=n, p=[0.6, 0.15, 0.08, 0.07, 0.05, 0.05])
    
    bytes_transferred = np.random.lognormal(mean=8, sigma=2, size=n).astype(int)
    bytes_transferred = np.clip(bytes_transferred, 100, 1000000)
    
    # Response times (ms)
    response_times = np.random.gamma(shape=2, scale=50, size=n).astype(int)
    response_times = np.clip(response_times, 1, 5000)
    
    # Geographic data
    countries = np.random.choice([
        "US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "CN"
    ], size=n, p=[0.3, 0.1, 0.1, 0.08, 0.07, 0.08, 0.05, 0.05, 0.07, 0.1])
    
    # Device types
    devices = np.random.choice([
        "Windows_Desktop", "MacOS_Laptop", "Linux_Server", "iPhone", 
        "Android", "iPad", "Router", "IoT_Device"
    ], size=n, p=[0.25, 0.15, 0.20, 0.12, 0.10, 0.08, 0.05, 0.05])
    
    data = {
        "timestamp": timestamps,
        "source_ip": source_ips,
        "destination_ip": dest_ips,
        "port": ports,
        "protocol": protocols,
        "event_type": event_types,
        "severity": severities,
        "user": users,
        "status_code": status_codes,
        "bytes": bytes_transferred,
        "response_time_ms": response_times,
        "country": countries,
        "device_type": devices,
        "session_id": np.random.randint(100000, 999999, size=n),
        "risk_score": np.random.uniform(0, 100, size=n).round(2)
    }
    
    df = pd.DataFrame(data)
    df.to_csv("data/synthetic_logs_test.csv", index=False)
    print(f"Generated {n} realistic synthetic logs with {len(data)} columns.")
    return df

if __name__ == "__main__":
    df = generate_logs(n=10_000_000)
    print(f"Dataset shape: {df.shape}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    del df  # Clean up to free memory
    print("Log generation complete.")
