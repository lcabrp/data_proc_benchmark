# 🚀 Data Processing Performance Benchmark

A comprehensive benchmarking suite that compares the performance of popular Python data processing libraries using real-world datasets. Perfect for data scientists, engineers, and researchers who want to make informed decisions about which library to use for their projects.

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

After running, open `data/benchmark_results.csv` to see detailed performance comparisons. Missing/unsupported libraries are recorded as "N/A" to avoid misleading zeros.

Supported input formats (auto‑detected): CSV (.csv, .csv.gz, .csv.zip, .csv.zst), Parquet (.parquet), JSON (.json), and NDJSON/JSONL (.ndjson/.jsonl).

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
| `benchmark_modular.py` | **Recommended** - Modular, universal format support, auto dataset detection | Most users |
| `benchmark_01.py` | Enhanced with cross-platform optimizations | Reference/compat |
| `benchmark.py` | **Reference** - Original implementation | High-memory systems, research comparisons |

## 🌐 Cross-Platform Compatibility

✅ **Windows**: Optimized Dask configuration, thread-based workers  
✅ **Linux**: Full library support including FireDucks  
✅ **macOS**: Complete compatibility with all libraries  
✅ **WSL2**: Tested and optimized for Windows Subsystem for Linux  

## 📊 Data Collection

Each benchmark run automatically collects:
- **System Information**: CPU, memory, platform details
- **Performance Metrics**: Execution times for each library/operation combination
- **Environment Details**: Python version, library versions
- **Results History**: All runs saved to CSV for trend analysis

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

Updated August 2025.
