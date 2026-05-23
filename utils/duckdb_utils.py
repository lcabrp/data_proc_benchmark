"""DuckDB connection helpers shared by benchmark scripts."""

import os
import platform
import tempfile
from pathlib import Path
from contextlib import contextmanager


def running_in_wsl() -> bool:
    """Return True when running inside WSL/WSL2."""
    release = platform.uname().release.lower()
    return "microsoft" in release or "wsl" in release or bool(os.environ.get("WSL_INTEROP"))


def connect_duckdb_for_benchmark(database: str = ":memory:"):
    """Create a DuckDB connection with benchmark-friendly stability settings.

    Optional environment overrides:
    - DATA_PROC_DUCKDB_TEMP_DIR: spill/temp directory. Defaults to the OS
      temp directory, e.g. %TEMP% on Windows or /tmp in WSL/Linux.
    - DATA_PROC_DUCKDB_MAX_TEMP: max spill size, e.g. 32GB
    - DATA_PROC_DUCKDB_THREADS: explicit DuckDB thread count
    - DATA_PROC_DUCKDB_MEMORY_LIMIT: explicit memory limit, e.g. 12GB
    """
    import duckdb

    con = duckdb.connect(database)
    con.execute("SET preserve_insertion_order = false")

    temp_dir = os.environ.get("DATA_PROC_DUCKDB_TEMP_DIR")
    if not temp_dir:
        temp_dir = str(Path(tempfile.gettempdir()) / "data-proc-benchmark-duckdb")
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    con.execute("SET temp_directory = ?", [temp_dir])

    max_temp = os.environ.get("DATA_PROC_DUCKDB_MAX_TEMP")
    if max_temp:
        con.execute("SET max_temp_directory_size = ?", [max_temp])

    threads = os.environ.get("DATA_PROC_DUCKDB_THREADS")
    if threads:
        con.execute("SET threads = ?", [int(threads)])

    memory_limit = os.environ.get("DATA_PROC_DUCKDB_MEMORY_LIMIT")
    if memory_limit:
        con.execute("SET memory_limit = ?", [memory_limit])

    return con


def duckdb_table_expr(path: str | Path) -> str:
    """Return a DuckDB table expression for a supported dataset file."""
    path = Path(path)
    escaped = str(path).replace("'", "''")
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return f"read_parquet('{escaped}')"
    if suffix in {".json", ".jsonl", ".ndjson"}:
        return f"read_json_auto('{escaped}')"
    return f"read_csv_auto('{escaped}')"


class DuckDBBenchmarkSource:
    """Manage DuckDB file-scan or cached-table benchmark modes."""

    def __init__(self, mode: str = "file", table_name: str = "benchmark_data") -> None:
        self.mode = mode
        self.table_name = table_name
        self._conn = None

    def set_mode(self, mode: str) -> None:
        if mode not in {"file", "cached"}:
            raise ValueError(f"Unsupported DuckDB benchmark mode: {mode}")
        self.mode = mode

    def prepare(self, path: str | Path) -> float | None:
        """Load the source into a DuckDB temp table when running in cached mode."""
        if self.mode != "cached":
            return None

        import time

        self.close()
        start = time.perf_counter()
        self._conn = connect_duckdb_for_benchmark()
        expr = duckdb_table_expr(path)
        self._conn.execute(f"CREATE OR REPLACE TEMP TABLE {self.table_name} AS SELECT * FROM {expr}")
        return time.perf_counter() - start

    @contextmanager
    def query(self, path: str | Path):
        """Yield a DuckDB connection and source expression for one query."""
        if self.mode == "cached" and self._conn is not None:
            yield self._conn, self.table_name
            return

        conn = connect_duckdb_for_benchmark()
        try:
            yield conn, duckdb_table_expr(path)
        finally:
            conn.close()

    def close(self) -> None:
        """Close any cached DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
