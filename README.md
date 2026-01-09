# 🚀 Data Processing Performance Benchmark

A comprehensive benchmarking suite that compares the performance of popular Python data processing libraries using real-world datasets. Perfect for data scientists, engineers, and researchers who want to make informed decisions about which library to use for their projects. Now includes per‑operation memory delta tracking (RSS) and optimized complex join algorithms to reduce transient peak usage.

This project now includes a modular benchmark runner with universal file-format support and automatic dataset detection.

## 🎯 What This Project Does

This benchmark tests popular data processing libraries against multi‑million record synthetic log datasets, measuring performance across four realistic operation types.

### 📚 Libraries Compared

| Library | Description | Best For |
|---------|-------------|----------|
| **🐼 Pandas** | The standard data analysis library | General data manipulation, small to medium datasets |
| **⚡ Modin** | Drop-in pandas replacement with parallelization | Scaling pandas operations to larger datasets |
| **🦀 Polars** | Rust-powered DataFrame library | High performance, memory efficiency |
| **🦆 DuckDB** | In-process analytical database | SQL analytics, OLAP operations |
| **🔥 FireDucks** | High-performance pandas alternative | Enterprise-scale data processing (Linux/macOS) |

### 🧪 Benchmark Operations

1. **🔍 Filter & Group**: Filter by status and group by source IP
2. **📊 Statistics**: Aggregations by event type (mean, std, min, max, medians)
3. **🔗 Complex Join**: Enrich rows with per‑source aggregates and rankings
4. **⏰ Time Series**: Hourly rollups (with fallback if timestamps are missing)

## 🚀 Quick Start

### 📦 Method 1: Using uv (Recommended)

**uv** is a lightning-fast Python package manager that makes setup incredibly quick.

1. **Install uv** (choose the easiest option):
   ```bash
   # Option A: Simple pip install (easiest if you have Python)
   pip install uv
   
   # Option B: Official installer  
   # Windows (PowerShell)
   irm https://astral.sh/uv/install.ps1 | iex
   
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and Setup**:
   ```bash
   git clone <repository-url>
   cd data_proc_benchmark
   uv sync  # Creates virtual environment and installs all dependencies
   # If 'uv' command not found, use: python -m uv sync
   ```

3. **Run the Modular Benchmark**:
   ```bash
   uv run python scripts/benchmark/benchmark_modular.py
   # If 'uv' is not on PATH:
   python -m uv run python scripts/benchmark/benchmark_modular.py
   ```

### 📦 Method 2: Traditional pip (Windows/macOS/Linux)

If you prefer the traditional Python workflow:

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd data_proc_benchmark
   ```

2. **Create Virtual Environment**:
   ```bash
   # Create virtual environment
   python -m venv .venv
   
   # Activate it
   # Windows:
   .venv\Scripts\activate
   # macOS/Linux:
   source .venv/bin/activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -e .
   # If psutil is missing (for host info collection):
   pip install psutil
   ```

4. **Run the Benchmark (PowerShell on Windows)**:
   ```powershell
   .\.venv\Scripts\python scripts/benchmark/benchmark_modular.py
   ```
   On macOS/Linux:
   ```bash
   python scripts/benchmark/benchmark_modular.py
   ```

### 📊 View Your Results

After running, open `data/benchmark_results.csv` to see detailed performance comparisons. Missing/unsupported libraries are recorded as "N/A" to avoid misleading zeros. For the modular benchmark, additional columns report per‑operation memory delta (RSS MB) where collected.

Supported input formats (auto‑detected): CSV (.csv, .csv.gz, .csv.zip, .csv.zst), Parquet (.parquet), JSON (.json), and NDJSON/JSONL (.ndjson/.jsonl).

Stored output now also includes dataset file metadata columns: `dataset_name` (filename) and `dataset_format` (csv, parquet, json, ndjson). This lets you directly compare performance across storage formats.

### 🧭 Compare Two Hosts (PC vs PC)

Use the host-to-host comparison CLI to compare two exact hostnames present in your benchmark results CSV. It prints overall, per-OS (Windows/WSL2/Linux), and per-format (CSV/Parquet) verdicts and can export a structured report.

PowerShell (Windows):

```powershell
# JSON export (single JSON document)
.\.venv\Scripts\python scripts\tools\compare_hosts.py `
   --csv data\benchmark_results.csv `
   --host ZBookFuryG8 `
   --host ZBookFuryG9 `
   --tie-threshold-pct 5 `
   --formats csv parquet `
   --libs pandas,polars,duckdb,fireducks `
   # --json-out is optional; defaults to data\results\compare_<A>_vs_<B>.json
   --json-out data\results\compare_ZBookFuryG8_vs_ZBookFuryG9.json

# NDJSON export (one record per line; streaming/append friendly)
.\.venv\Scripts\python scripts\tools\compare_hosts.py `
   --csv data\benchmark_results.csv `
   --host ZBookFuryG8 `
   --host ZBookFuryG9 `
   --tie-threshold-pct 5 `
   --formats csv parquet `
   --libs pandas,polars,duckdb,fireducks `
   # When --ndjson is set, default filename uses .ndjson if --json-out omitted
   --json-out data\results\compare_ZBookFuryG8_vs_ZBookFuryG9.ndjson `
   --ndjson

# Custom output directory
.\.venv\Scripts\python scripts\tools\compare_hosts.py `
   --csv data\benchmark_results.csv `
   --host ZBookFuryG9 `
   --host IdeaPadPro5i `
   --formats csv parquet `
   --libs pandas,polars,duckdb,fireducks `
   --out-dir data\custom_results

# No export (console only)
.\.venv\Scripts\python scripts\tools\compare_hosts.py `
   --csv data\benchmark_results.csv `
   --host ZBookFuryG9 `
   --host IdeaPadPro5i `
   --formats csv parquet `
   --libs pandas,polars,duckdb,fireducks `
   --no-export

# Quiet Mode: print only Summary + Verdict
.\.venv\Scripts\python scripts\tools\compare_hosts.py `
   --csv data\benchmark_results.csv `
   --host ZBookFuryG8 `
   --host ZBookFuryG9 `
   --tie-threshold-pct 5 `
   --formats csv parquet `
   --libs pandas,polars,duckdb,fireducks `
   --quiet
```

Notes:
- Exact hostnames only; if a name is wrong, the tool errors with suggestions.
- Positive percentage deltas mean the second host (`--host` B) is faster.
- Use `--libs`/`--formats` to scope analysis; omit to auto-include available ones.
- `--json-out` is optional; defaults to `data/results/compare_<hostA>_vs_<hostB>.json` (or `.ndjson` when `--ndjson`).
- Reports start with a one-line headline winner and a Summary block; use `--quiet` for a concise view.
- `--out-dir` sets the directory used for inferred filenames when `--json-out` is omitted.
- `--no-export` skips writing JSON/NDJSON and prints to console only.

Load the report in Python:

```python
# JSON (single object)
import pandas as pd
json_obj = pd.read_json('data/results/compare_ZBookFuryG8_vs_ZBookFuryG9.json', typ='series').to_dict()

# NDJSON (one object per line into a DataFrame)
import pandas as pd
df = pd.read_json('data/results/compare_ZBookFuryG8_vs_ZBookFuryG9.ndjson', lines=True)

# Polars examples
import polars as pl
json_series = pl.read_json('data/results/compare_ZBookFuryG8_vs_ZBookFuryG9.json')
ndjson_df = pl.read_ndjson('data/results/compare_ZBookFuryG8_vs_ZBookFuryG9.ndjson')
```

JSON vs NDJSON:
- JSON is a single document, easy for ad-hoc loading, emailing, or artifact storage.
- NDJSON is line-delimited and better for streaming/append pipelines and ingest into systems like Elasticsearch, ClickHouse, or `jq`/shell processing.

### 🆘 First-Time Setup Help

**New to Python development?** Here's what you need:

1. **Python 3.13+**: Download from [python.org](https://python.org) if you don't have it
2. **Git**: Download from [git-scm.com](https://git-scm.com) for cloning repositories
3. **Choose your method**: 
   - **uv** = Fast and modern (recommended for new projects)
   - **pip** = Traditional and widely supported

**Common Issues:**
- **"python command not found"**: Make sure Python is in your PATH
- **"git command not found"**: Install Git and restart your terminal
- **"uv command not found"**: If uv isn't in your PATH after installation, use `python -m uv` instead of `uv`
- **Permission errors on Windows**: Run PowerShell as Administrator for uv installation

## 📈 Sample Results

```
FILTER_GROUP Operation:
  Fastest: polars (1.55s)
  polars    :   1.55s (x1.0)
  duckdb    :   4.04s (x0.4)
  pandas    :  22.92s (x0.1)
  modin     :  25.46s (x0.1)

TIMESERIES Operation:
  Fastest: duckdb (5.25s)
  duckdb    :   5.25s (x1.0)
  polars    :   9.04s (x0.6)
  pandas    :  27.26s (x0.2)
  modin     :  37.55s (x0.1)
```

## 🎛️ Available Benchmark Scripts

| Script | Purpose | Best For |
|--------|---------|----------|
| `benchmark_modular.py` | **Recommended** - Modular, universal format support, auto dataset detection, memory deltas | Most users |
| `benchmark_01.py` | Enhanced with cross-platform optimizations (unified CLI) | Reference/compat |
| `benchmark_02.py` | Reliability + universal format + log suppression | Alternate runner |
| `benchmark.py` | **Reference** - Original implementation (baseline kept) | High-memory systems, research comparisons |

### Unified CLI Flags
All active benchmark scripts now use a single flag set:

```
   -d / --dataset          Path to dataset file (optional if auto-detect applies)
   -o / --output           Results CSV output path (default: data/benchmark_results.csv)
   --optimize / -opt       Memory optimization mode: auto (default), always, or never
   --mem-threshold / -m    Memory threshold in GB for auto mode (default: 16)
   --repeat N              Repeat each operation N times (where supported; default: 1)
```

#### Memory Optimization Control

The `--optimize` flag provides flexible control over memory optimization for pandas/FireDucks:

- **`auto`** (default): Automatically optimizes when system memory < threshold (16GB default)
- **`always`**: Forces optimization regardless of system memory (useful for testing)
- **`never`**: Disables optimization even on low-memory systems (useful for benchmarking raw performance)

Examples:
```bash
# Auto mode with default 16GB threshold
python scripts/benchmark/benchmark.py -d data.csv

# Force optimization on high-memory system
python scripts/benchmark/benchmark.py -d data.csv --optimize always

# Disable optimization to test raw performance
python scripts/benchmark/benchmark.py -d data.csv --optimize never

# Auto mode with custom 32GB threshold
python scripts/benchmark/benchmark.py -d data.csv --optimize auto -m 32
```

**Impact**: Memory optimization provides 2-3x speedup and 94% memory reduction for pandas/FireDucks on systems below the threshold.

Legacy flags `--csv` and `--results` were removed early for consistency.

## 🌐 Cross-Platform Compatibility

✅ **Windows**: Optimized Dask configuration, thread-based workers  
✅ **Linux**: Full library support including FireDucks  
✅ **macOS**: Complete compatibility with all libraries  
✅ **WSL2**: Tested and optimized for Windows Subsystem for Linux  

## 📊 Data Collection

Each benchmark run automatically collects:
- **System Information**: CPU, memory, platform details
- **Performance Metrics**: Execution times for each library/operation combination (and memory deltas in modular runner)
- **Environment Details**: Python version, library versions
- **Results History**: All runs saved to CSV for trend analysis
 - **Dataset Metadata**: Dataset filename (`dataset_name`) and normalized format (`dataset_format`)

## 🎯 Use Cases

- **Library Selection**: Choose the best library for your data size and operations
- **Performance Monitoring**: Track performance changes across different environments
- **Hardware Planning**: Understand how different hardware affects data processing speed
- **Research & Development**: Compare optimization strategies and configurations

## 📚 Documentation

- **[Technical Details](TECHNICAL.md)**: Deep dive for engineers (Modin setup, architecture, modules)
- **[Data Generation](scripts/log-gen/)**: Synthetic dataset creation using `test_generator_01.py`
- **[Results Analysis](data/)**: CSV output format and analysis guidelines

### 🔧 Converting CSV/NDJSON to Parquet
Parquet is usually smaller and faster to read. Use our helper:

```powershell
# CSV -> Parquet (snappy)
.\.venv\Scripts\python scripts/tools/csv_to_parquet.py --input data\raw\logs.csv --out data\raw\logs.parquet

# Compressed CSV -> Parquet (zstd)
.\.venv\Scripts\python scripts/tools/csv_to_parquet.py --input data\raw\logs.csv.gz --out data\raw\logs.parquet --compression zstd

# NDJSON/JSONL -> Parquet
.\.venv\Scripts\python scripts/tools/csv_to_parquet.py --input data\raw\logs.ndjson --out data\raw\logs.parquet --format ndjson
```

### 🧩 Utilities overview
- `utils/host_info.py`: Collects system details (CPU/mem/Python). Requires `psutil` and optionally `py-cpuinfo`.
- `utils/data_io.py`: Universal readers and helpers:
   - `UniversalDataReader`: read CSV/Parquet/JSON/NDJSON via pandas, modin, polars, or DuckDB
   - `DatasetFinder`: locate the best dataset automatically
   - `get_dataset_size(path)`: count records efficiently

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add new operations or library support
4. Test across different platforms
5. Submit a pull request

## 📄 License

This project is open source. See LICENSE file for details.

---

**💡 Pro Tip**: Start with `benchmark_01.py` for reliable cross-platform results, then experiment with other versions based on your specific needs!

Updated September 2025.
