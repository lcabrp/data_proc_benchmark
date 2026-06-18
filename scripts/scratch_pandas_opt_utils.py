# Created by Gemma4:12B

import time
import gc
from dataclasses import dataclass, field
from typing import Dict, Optional
from contextlib import ContextManager
from typing_extensions import TypedDict

@dataclass
class BenchmarkResult:
    """Data class to hold results of a single benchmark run."""
    load_time: float
    deep_mem: float
    process_mem_delta: float
    timings: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert the result object to a dictionary for easy export."""
        res = {
            "load_time": self.load_time,
            "deep_mem": self.deep_mem,
            "process_mem_delta": self.process_mem_delta,
        }
        res.update(self.timings)
        return res

class Timer:
    """Context manager to measure execution time and trigger GC."""
    def __init__(self, name: str):
        self.name = name
        self.start_time = 0.0
        self.elapsed = 0.0

    def __enter__(self):
        gc.collect()
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.perf_counter() - self.start_time