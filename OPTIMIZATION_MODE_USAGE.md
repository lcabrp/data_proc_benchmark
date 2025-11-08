# Memory Optimization Mode Usage Guide

## Overview
The benchmark script now supports flexible memory optimization control through the `--optimize` flag.

## Command Line Options

### `--optimize` / `-opt` (choices: auto, always, never)
Controls when memory optimization is applied to pandas/fireducks DataFrames.

**Default:** `auto`

### `--mem-threshold` / `-m` (float)
Memory threshold in GB for auto mode.

**Default:** `16` (GB)

## Usage Examples

### 1. Auto Mode (Default)
Automatically optimize based on system memory threshold:
```bash
# Uses default 16GB threshold
python scripts/benchmark/benchmark.py -d data.csv -o results.csv

# Explicitly specify auto mode
python scripts/benchmark/benchmark.py -d data.csv -o results.csv --optimize auto

# Auto mode with custom 32GB threshold
python scripts/benchmark/benchmark.py -d data.csv -o results.csv --optimize auto -m 32
```

**Behavior:**
- System < 16GB RAM → Optimization applied
- System ≥ 16GB RAM → Optimization skipped

### 2. Always Mode
Force optimization regardless of system memory:
```bash
python scripts/benchmark/benchmark.py -d data.csv -o results.csv --optimize always
```

**Use cases:**
- Testing optimized performance on high-RAM systems
- Comparing optimized vs non-optimized results
- Ensuring consistent behavior across different machines

### 3. Never Mode
Disable optimization even on low-memory systems:
```bash
python scripts/benchmark/benchmark.py -d data.csv -o results.csv --optimize never
```

**Use cases:**
- Benchmarking raw (non-optimized) performance
- Testing memory usage patterns
- Comparing against optimized runs

## Test Scripts

### Linux/Mac (bash)
```bash
./benchmark_test_sh.sh
```

### Windows (batch)
```bat
benchmark_test_win.bat
```

Both scripts run 4 tests:
1. Auto mode (default)
2. Always optimize
3. Never optimize
4. Auto mode with custom 32GB threshold

## Result Tracking

Results are tracked in CSV with enhanced script names:

- `benchmark.py_opt_always` - Always mode
- `benchmark.py_opt_never` - Never mode
- `benchmark.py_opt_auto_mem15GB` - Auto mode, optimized on 15GB system
- `benchmark.py_no_opt_auto_mem64GB` - Auto mode, not optimized on 64GB system

## Output Messages

The script displays clear information about optimization decisions:

```
Memory optimization settings:
  - Mode: auto (optimize if system memory < 16.0GB)

Loading and optimizing data for pandas...
  System has 15.3GB RAM (< 16.0GB threshold) - applying optimization
  pandas DataFrame optimized: 94.0% memory reduction (5518.3MB → 333.8MB)
```

Or:

```
Memory optimization settings:
  - Mode: always (forced optimization)

Loading and optimizing data for pandas...
  System has 64.0GB RAM - optimization FORCED via --optimize always
  pandas DataFrame optimized: 94.0% memory reduction (5518.3MB → 333.8MB)
```

## Migration from Old Flags

**Old:** `--force-optimize` / `-f`  
**New:** `--optimize always`

**Old:** (no flag, auto-detected)  
**New:** `--optimize auto` (or omit for default)

**Old:** (no equivalent)  
**New:** `--optimize never`
