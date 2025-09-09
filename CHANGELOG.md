# Changelog

All notable changes to this project will be documented in this file.

The format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and uses semantic versioning when feasible.

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
