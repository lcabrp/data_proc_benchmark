# HP ZBook Studio G8 (11th Gen i7) vs WL5040 (Ryzen 9 8945HS)

This report compares the performance of the **HP ZBook Studio G8** (equipped with an 11th Gen Intel Core i7-11850H) against the **Lenovo WL5040** (equipped with the newer AMD Ryzen 9 8945HS).

Both laptop processors feature an identical configuration of 8 physical cores and 16 logical threads.

## Verdict
**Winner: ZBook Studio G8** (~33.4% faster overall)

Surprisingly, despite utilizing a significantly older 11th Gen Intel architecture, the ZBook Studio G8 consistently outperformed the much newer AMD Ryzen 9 8945HS across these data processing benchmarks.

## Overall Performance by Library

On average across all operations and datasets:

| Library | Winner | Difference |
| :--- | :--- | :--- |
| **pandas** | **ZBook Studio G8** | ~15.6% faster |
| **polars** | **ZBook Studio G8** | ~25.8% faster |
| **duckdb** | **ZBook Studio G8** | ~44.9% faster |
| **fireducks** | **ZBook Studio G8** | ~328.4% faster |

The 11th Gen Intel system maintained its lead across every data processing library tested. The performance gap grew drastically when evaluating highly vectorized and multi-threaded engines like DuckDB and FireDucks, reaching up to a massive 328% advantage in FireDucks.

## Operating System Breakdown

The AMD system's performance heavily depends on the OS environment:

*   **Windows (Native):** ZBook is **~16.2% faster**. The gap is noticeable but much closer in a native environment.
*   **WSL2:** ZBook is **~56.8% faster**. The AMD Ryzen system struggled significantly under the WSL2 virtualization layer, destroying its performance relative to the Intel-based ZBook.

## Advanced Analysis & Methodology

*   **Performance Stability:** Unlike the Intel system, the AMD-powered WL5040 proved to be incredibly consistent and stable across benchmark runs (Coefficient of Variation: just 8.2% vs the ZBook's 20.0%). However, despite this high stability, the ZBook's worst recorded times were still generally faster than the WL5040's best times.
*   **Memory Efficiency:** The ZBook Studio G8 uses its memory footprint far more efficiently, logging an average operation time of 0.1331 s/GB versus the WL5040's 0.2276 s/GB.
*   **Architecture & Optimization:** The extreme disparity, particularly in tools like `duckdb` and `fireducks` under WSL2, strongly indicates that these libraries are heavily optimizing around Intel-specific instruction sets (like AVX-512 or specific MKL routines) which execute far more slowly or require emulation passes on the AMD Zen 4 architecture.
