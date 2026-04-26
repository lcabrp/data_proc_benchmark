# Lenovo Mini PC (i7-12700T) vs ZBook Power G9 (i7-12700H) Benchmark Comparison

This report compares the performance of the **Lenovo ThinkCentre Mini PC** (hostname `LnvThkCtr-01`, equipped with the power-efficient 12th Gen Intel Core i7-12700T) against the **HP ZBook Power G9** laptop (hostname `ZBookPowerG9`, equipped with the high-performance 12th Gen Intel Core i7-12700H).

> [!NOTE]
> *The dataset contains the HP ZBook Power G9 with the i7-12700H, which matches your CPU specification. ZBook Studio models in the dataset are equipped with 11th Gen processors.*

## Verdict
**Winner: ZBookPowerG9** (~14.8% faster overall)

Despite being a laptop, the ZBook Power G9's H-series processor (higher TDP for performance) consistently outperforms the Lenovo Mini PC's T-series processor (low power, designed for compact desktops). 

Both processors share the exact same underlying architecture and core counts (14 cores / 20 threads: 6 P-cores and 8 E-cores), demonstrating how power and thermal limits directly constrain performance.

## Overall Performance by Library

On average across all operations and datasets:

| Library | Winner | Difference |
| :--- | :--- | :--- |
| **pandas** | **Tie** | Within 5.0% threshold (~0.4% diff) |
| **polars** | **ZBookPowerG9** | ~16.6% faster |
| **duckdb** | **ZBookPowerG9** | ~21.9% faster |
| **fireducks** | **ZBookPowerG9** | ~74.6% faster |

The performance gap is most pronounced when using highly multi-threaded libraries like DuckDB and FireDucks. The ZBook's higher power limits allow its 14 cores to sustain higher boost frequencies under full multi-core loads. In contrast, with less parallel pandas workloads, the two systems hit similar clocks and perform virtually identically.

## Operating System Breakdown

The performance advantage of the ZBook Power G9 varies significantly depending on the OS environment:

*   **Linux (Native):** ZBook is **~8.1% faster** (smallest gap).
*   **Windows:** ZBook is **~23.8% faster**. 
*   **WSL2:** ZBook is **~37.0% faster** (largest gap). The ZBook's higher power envelope appears to handle the WSL2 virtualization overhead significantly better, particularly in highly parallel workloads.

## Advanced Analysis & Methodology

*   **Memory Efficiency:** The ZBook Power G9 processes data more efficiently per GB of memory (0.0298 s/GB vs 0.0386 s/GB).
*   **Stability:** Both hosts show similar performance stability and consistency across runs (Coefficient of Variation: ~24.7% for Lenovo vs 23.8% for ZBook).
*   **Fair Comparison:** The comparison script successfully normalized and filtered the benchmark runs to the overlapping 64GB physical RAM range, eliminating memory capacity disparities.
