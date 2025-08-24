# 🚀 Data Processing Performance Benchmark

A comprehensive benchmarking suite that compares the performance of popular Python data processing libraries using real-world datasets. Perfect for data scientists, engineers, and researchers who want to make informed decisions about which library to use for their projects.

## 🎯 What This Project Does

This benchmark tests **5 major data processing libraries** against a **10-million record synthetic log dataset**, measuring performance across **4 different operation types** that mirror real-world data analysis tasks.

### � Libraries Compared

| Library | Description | Best For |
|---------|-------------|----------|
| **🐼 Pandas** | The standard data analysis library | General data manipulation, small to medium datasets |
| **⚡ Modin** | Drop-in pandas replacement with parallelization | Scaling pandas operations to larger datasets |
| **🦀 Polars** | Rust-powered DataFrame library | High performance, memory efficiency |
| **🦆 DuckDB** | In-process analytical database | SQL analytics, OLAP operations |
| **🔥 FireDucks** | High-performance pandas alternative | Enterprise-scale data processing (Linux/macOS) |

### 🧪 Benchmark Operations

1. **🔍 Filter & Group**: Find the top bandwidth consumers by filtering active connections and grouping by source IP
2. **📊 Statistical Analysis**: Generate comprehensive statistics across different event types
3. **🔗 Complex Joins**: Create enriched datasets with rankings and aggregated metrics
4. **⏰ Time Series**: Analyze patterns across hourly time buckets

## 🚀 Quick Start

### 📦 Method 1: Using uv (Recommended - Faster!)

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

3. **Run the Enhanced Benchmark**:
   ```bash
   uv run python scripts/benchmark/benchmark_01.py
   # If 'uv' command not found, use: python -m uv run python scripts/benchmark/benchmark_01.py
   ```

### 📦 Method 2: Traditional pip Approach

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
   ```

4. **Run the Benchmark**:
   ```bash
   python scripts/benchmark/benchmark_01.py
   ```

### 📊 View Your Results

After running either method above, open `data/benchmark_results.csv` to see detailed performance comparisons across all libraries.

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
| `benchmark_01.py` | **Production** - Enhanced with cross-platform optimizations | **Most users** - reliable results across Windows/Linux/macOS |
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

- **[Technical Details](TECHNICAL.md)**: Deep dive into implementation, optimizations, and benchmark evolution
- **[Data Generation](scripts/log-gen/)**: Synthetic dataset creation using `test_generator_01.py`
- **[Results Analysis](data/)**: CSV output format and analysis guidelines

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
