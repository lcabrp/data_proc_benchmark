# Changelog

All notable changes to this project will be documented in this file.

The format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and uses semantic versioning when feasible.
## [0.1.4] - 2025-09-20
### Added
- FireDucks Support: Added full support for FireDucks (Linux/macOS only) across all benchmark scripts, including operations, loading, optimization, and CSV output.
- Memory Usage Tracking: Integrated detailed memory usage logging using df_memory_usage from useful_functions.py for pandas and FireDucks optimizations.
- Single-Load Caching: Implemented efficient single-load per library in benchmark_modular.py to load/optimize datasets once per library, reducing memory usage and improving performance.
- Result Trimming: Added logic to trim large result DataFrames (e.g., keeping only the first 10 rows for results >1000 rows) to prevent memory bloat during benchmarking.
- Missing Methods: Added _duckdb_table_expr() method for DuckDB operations in benchmark_modular.py.
Changed
- Operation Alignment: Standardized all benchmark operations (filter_group, statistics, complex_join, timeseries) across benchmark.py, benchmark_01.py, benchmark_02.py, and benchmark_modular.py to ensure identical semantics and results.
- CSV Output Consistency: Ensured all scripts save results with full precision (no truncation), blanks ("") for unavailable libraries, and the same 16-column format. Fixed I/O issues in benchmark_modular.py by moving writer.writerow(row) inside the with block.
- Library Removal: Removed Modin and Dask from benchmark_modular.py to focus on pandas, polars, duckdb, and fireducks.
- Import Fixes: Updated imports to use UniversalDataReader for file format detection and optimize_df_types for memory optimization.
- Error Handling: Improved robustness with better exception handling, availability checks, and logging for missing libraries or operations.
Fixed
- Import Errors: Resolved NameError: name 'detect_file_format' is not defined by using UniversalDataReader.detect_file_format().
- Syntax Errors: Fixed try-except blocks and indentation issues in benchmark_modular.py.
- Memory Management: Ensured DataFrames are explicitly deleted (del cached_df; gc.collect()) after each library's operations to free memory sequentially.
- CSV Saving: Corrected file closure issues to prevent "I/O operation on closed file" errors.
- Optimization Failures: Handled cases where optimize_df_types fails gracefully with warnings.
Removed
- Modin/Dask Dependencies: Completely removed Modin and Dask from benchmark_modular.py, including related functions, imports, and setup/cleanup code.
- Duplicate Code: Cleaned up duplicate methods and functions in benchmark_modular.py to use the single-load caching version.
Performance
- Memory Efficiency: Achieved up to 94% memory reduction for pandas optimizations (e.g., from 5518MB to 315MB for 10M rows).
- Benchmark Speed: Improved execution times, especially for polars and duckdb, with sequential library processing reducing overall memory footprint.
- Compatibility: Verified across multiple systems (Windows, Linux, macOS) and datasets (CSV, Parquet).
Notes
- FireDucks is limited to Linux/macOS; skipped gracefully on Windows.
- Large datasets (e.g., 10M rows) may still consume significant memory during complex_join operations; trimming helps but doesn't eliminate it entirely.
- Version bump is internal (pyproject still at 0.1.0 until next release tag). Update pyproject.toml when publishing.

## [0.1.3] - 2025-09-08
### Added
- Per‑operation resident set size (RSS) memory delta tracking in `benchmark_modular.py`.
- Optimized complex join implementations:
    - Pandas & Modin now use in‑place groupby `transform` pattern (avoids large intermediate join DataFrame).
    - Polars version rewritten as a single lazy pipeline with window functions (no materialized self‑join).
- Lazy Polars pipeline reduces unnecessary materialization prior to final top‑N filtering.

### Changed
- Complex join algorithms made more memory‑efficient; reduced transient peak allocations (qualitative – no fixed number asserted).
- README and TECHNICAL docs updated to describe memory instrumentation and optimization approach.

### Fixed
- Missing `complex_join` timings caused by earlier naive name splitting logic (now robust pairing logic – applied across scripts).
- Incorrect `dataset_name` / `dataset_format` derivation in one writer path (now always computed from runtime dataset path).

### Notes
- FireDucks still omitted on Windows; results recorded as `N/A` rather than `0` to avoid skewed comparisons.
- Next release should bump `pyproject.toml` version from 0.1.0 when publishing.

## [0.1.2] - 2025-09-08
### Added
- `dataset_name` and `dataset_format` columns added to all benchmark result writers (all script variants).
- Documentation (README, TECHNICAL.md) updated to describe new metadata.

### Notes
- Existing `data/benchmark_results.csv` produced prior to this version will be missing the new columns; mixing schemas may complicate analysis. Consider archiving or regenerating.

### Removed
- Legacy flags `--csv` and `--results` (previously only in `benchmark_01.py`).

### Changed
- Scripts table in README now lists all four variants including `benchmark_02.py`.
- `benchmark_01.py` simplified argument parsing (deprecated aliases dropped).

### Notes
- Version bump is internal (pyproject still at 0.1.0 until next release tag). Update `pyproject.toml` when publishing.
- Original `benchmark.py` retained as a baseline reference implementation.

## [0.1.1] - 2025-09-08
### Added
- Unified CLI flags across all benchmark scripts (`-d/--dataset`, `-o/--output`, `--repeat`).
- Documentation updates (README, TECHNICAL.md) reflecting the single interface.
- `benchmark_modular.py` and `benchmark_01.py` aligned with `benchmark.py` / `benchmark_02.py` parameter model.

## [0.1.0] - 2025-08-??
### Initial
- Initial public structure with multiple benchmark script variants and utilities.

## [Unreleased]
### Added
- Single dataset load and optimization per library in `benchmark_01.py` (pandas, polars, and FireDucks when available) for improved efficiency and fair comparisons.
- FireDucks support in `benchmark_01.py` when available (e.g., on Linux), with automatic detection and optimized loading.
- Comprehensive type hints, docstrings, and error handling across `benchmark_01.py` for robustness and maintainability.

### Changed
- Operations in `benchmark_01.py` now exactly match `benchmark.py` semantics (filter_group: filter bytes > 1000 and count; statistics: group by event_type with mean/min/max; complex_join: sum bytes by source_ip, rank by total_bytes; timeseries: extract hour and count).
- Column type optimizations in `benchmark_01.py` aligned with `benchmark.py` (port as uint16, not category).
- CSV output in `benchmark_01.py` uses blanks for unavailable libraries instead of `np.nan` or "N/A", ensuring clean, consistent schemas.
- Timing precision in CSV output increased to 15 significant digits for accurate comparisons.

### Fixed
- CSV schema consistency in `benchmark_01.py`: All library columns (including FireDucks) are always included, with blanks for unavailable results—no missing columns or misalignments.
- Removed duplicate code, logical errors, and inconsistencies in `benchmark_01.py` (e.g., unified operation implementations, proper caching).
- DuckDB operations in `benchmark_01.py` now read directly from file (no redundant in-memory loads) for isolation.

### Notes
- `benchmark_01.py` is now fully aligned with `benchmark.py` for equivalent performance testing across libraries.
- FireDucks results are skipped gracefully on unsupported platforms (e.g., Windows), with blanks in CSV.
