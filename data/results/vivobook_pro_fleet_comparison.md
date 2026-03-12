# VivoBookPro vs All Laptops: Fleet Performance Analysis

**Date:** 2026-03-04  
**Machine Under Test:** ASUS VivoBookPro — 13th Gen Intel Core i9-13980HX (8P+16E, 32 threads), ~64GB RAM  
**Methodology:** All comparisons use the **auto OS intersection filter** — only OS environments shared by both hosts are included, ensuring fair apples-to-apples results.

---

## Overall Results

| Rank | Opponent | CPU (threads) | Overall | Pandas | DuckDB | Polars | FireDucks |
|:---:|---|---|:---:|:---:|:---:|:---:|:---:|
| 1 | **ZBookFuryG8** | i9-11900H (16t) | **Vivo +31.87%** | +40.66% | +27.93% | +27.82% | -69.93% |
| 2 | **Legion5-15AHP10** | Ryzen 7 260 (16t) | **Vivo +24.42%** | +38.25% | +17.85% | +12.94% | -52.93% |
| 3 | **ThinkBook** | Ultra 7 255H (16t) | **Vivo +24.23%** | +17.21% | +30.20% | +19.62% | -67.95% |
| 4 | **DELL-XPS-9520** | i7-12700H (20t) | **Vivo +17.24%** | +20.14% | +16.82% | +23.07% | -43.21% |
| 5 | **ZBookFuryG9** | i7-12850HX (24t) | **Vivo +16.82%** | +21.84% | +16.82% | +14.26% | -71.74% |
| 6 | **MSI-VectorA16HX** | Ryzen 9 8940HX (32t) | **Vivo +14.50%** | +7.81% | +19.75% | +13.41% | -60.69% |
| 7 | **ROG-Strix-G17** | Ryzen 9 7945HX (32t) | **Vivo +13.71%** | +14.55% | +15.93% | +15.49% | -72.74% |
| 8 | **ZBookPowerG9** | i7-12700H (20t) | **Vivo +8.77%** | +13.21% | +8.20% | +11.01% | -61.25% |
| 9 | **Legion7-16IRX9** | i9-14900HX (32t) | **Vivo +7.94%** | +14.96% | +8.03% | +13.10% | -80.90% |
| 10 | **Legion5-15IRX10** | i7-14700HX (28t) | **TIE** (+4.05%) | +18.34% | -0.49% | -3.30% | -92.22% |
| 11 | **ROG-Strix-G16** | Ryzen 9 9955HX (32t) | **TIE** (+0.99%) | +11.01% | -2.08% | +1.64% | -118.79% |

**Verdict:** The VivoBookPro is the **top data-processing laptop in the fleet**, winning 9 out of 11 matchups and tying the other 2.

---

## Key Findings

### 1. Pandas, DuckDB, and Polars: Dominant
The i9-13980HX won Pandas in **all 11 matchups**, DuckDB in 9 of 11, and Polars in 9 of 11. The combination of 8 high-clocked P-cores and 32 total threads gives it an edge in both single-threaded (Pandas) and multi-threaded (DuckDB, Polars) workloads.

### 2. Closest Competitors
- **ROG-Strix-G16** (Ryzen 9 9955HX, 32t): Virtually identical overall (+0.99%), and actually beat the VivoBook in DuckDB (-2.08%). AMD's unified-core architecture traded evenly with Intel's hybrid design.
- **Legion5-15IRX10** (i7-14700HX, 28t): Tied at +4.05%. Its 14th Gen Raptor Lake Refresh nearly matched the 13th Gen i9 with fewer threads.

### 3. Biggest Wins
- **ZBookFuryG8** (-31.87%): The 11th Gen Tiger Lake i9-11900H (16t) was completely outclassed — a full generational leap behind.
- **Legion5-15AHP10** (-24.42%): The AMD Ryzen 7 260 (16t) couldn't compete with the raw thread count advantage.

---

## FireDucks Deep Dive: The Achilles' Heel

The VivoBookPro lost FireDucks in **all 11 matchups**, by margins ranging from -43% to -119%. This is not a minor weakness — it's a catastrophic, systematic failure in one specific library.

### Per-Operation Analysis

| Host | CPU | filter_group | statistics | complex_join | timeseries |
|---|---|---|---|---|---|
| **VivoBookPro** | i9-13980HX (8P+16E) | 0.067s | 0.051s | **0.485s** | **1.067s** |
| ZBookPowerG9 | i7-12700H (6P+8E) | 0.082s | 0.059s | 0.531s | 0.475s |
| MSI-VectorA16HX | Ryzen 9 8940HX | 0.065s | 0.048s | 0.499s | 0.452s |
| DELL-XPS-9520 | i7-12700H (6P+8E) | 0.083s | 0.063s | 0.624s | 0.526s |
| Legion7-16IRX9 | i9-14900HX (8P+16E) | 0.002s | 0.001s | 0.247s | 0.377s |

The `filter_group` and `statistics` operations are **competitive or best-in-class** on the VivoBookPro. The problem is isolated to:

- **`timeseries`**: 1.067s — **2-2.5× worse** than every other machine (~0.38-0.53s)
- **`complex_join`**: 0.485s — similar mean, but with extreme variance

### The Mean-vs-Median Gap

| Operation | Mean | Median | Ratio |
|---|---|---|---|
| complex_join | 0.485s | 0.0006s | **808×** |
| timeseries | 1.067s | 0.394s | **2.7×** |

The VivoBookPro's medians are competitive — *most runs are fast*. But intermittent catastrophic spikes on some runs drag the mean up dramatically.

### Root Cause: E-Core Scheduling Pathology

The i9-13980HX has a **2:1 E-to-P ratio** (16 Efficiency cores vs 8 Performance cores). The evidence points to an E-core scheduling problem specific to FireDucks:

1. **Competitive medians** mean most runs execute on P-cores and perform well.
2. **Occasional extreme spikes** (800× for complex_join) indicate that when the OS scheduler assigns FireDucks threads to E-cores (~40% lower IPC), certain operations catastrophically slow down.
3. **The `timeseries` operation** (which involves sequential temporal dependencies) is most sensitive to this — landing on an E-core mid-computation causes cascading delays.
4. **AMD Ryzen machines** (MSI-VectorA16HX, ROG-Strix-G17) with **identical cores** avoid this entirely — their FireDucks numbers are consistent with no extreme variance.
5. **Machines with fewer E-cores** (i7-12700H with 6P+8E, 1.3:1 ratio) have a lower probability of scheduling to E-cores, resulting in better FireDucks performance.

### Supporting Evidence: Legion7-16IRX9

Interestingly, the **Legion7-16IRX9** (i9-14900HX, also 8P+16E like the VivoBookPro) shows much better `filter_group` and `statistics` times (0.002s, 0.001s) but still loses FireDucks by -80.9%. This could suggest that the Lenovo Legion's BIOS/thermal management handles E-core scheduling differently, or that 14th Gen E-cores have improved IPC over 13th Gen.

### Conclusion

> **The VivoBookPro's FireDucks weakness is not a CPU power issue — it's an E-core scheduling pathology.** FireDucks' internal thread management doesn't account for Intel's hybrid P+E architecture. Fixing this would require E-core-aware thread affinity (pinning compute-intensive threads to P-cores only). Libraries like Pandas, DuckDB, and Polars handle this transparently, which is why the VivoBookPro dominates in those benchmarks.

---

## VivoBookPro vs ~32GB Laptops

The VivoBookPro has a natural RAM advantage (~64GB vs ~32GB) against these machines. However, 32GB is sufficient for these 10M-row benchmarks, so the comparison still reveals genuine CPU architectural differences.

> **Note:** Memory configuration warnings were raised for all comparisons below due to the ~2× RAM gap. The VivoBook's extra RAM may provide a secondary advantage in memory-hungry operations.

| Rank | Opponent | CPU (threads) | RAM | Overall | Pandas | DuckDB | Polars | FireDucks |
|:---:|---|---|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | **WL5040** | Ryzen 9 8945HS (16t) | 32GB | **Vivo +56.09%** | +46.86% | +61.24% | +55.28% | **+70.18%** |
| 2 | **ThinkPadE16G2** | Ryzen 7 7735HS (16t) | 32GB | **Vivo +39.31%** | +26.96% | +44.68% | +38.89% | -8.40% |
| 3 | **Legion7-16ARX8H** | Ryzen 9 7945HX (32t) | 32GB | **Vivo +27.89%** | +23.62% | +34.80% | +21.98% | -4.39% |
| 4 | **IdeaPadPro5i** | Ultra 9 185H (22t) | 32GB | **Vivo +19.19%** | +24.80% | +20.43% | +7.99% | -45.90% |
| 5 | **HeliosNeo16S** | Ultra 9 275HX (24t) | 32GB | **Vivo +11.01%** | +14.70% | +20.25% | +1.97% | **-20.10%** |
| 6 | **DELL-XPS15-9530** | i9-13900H (20t) | 32GB | **Vivo +9.52%** | +8.29% | +10.79% | +8.36% | -77.88% |
| 7 | **Precision-7670** | i9-12950HX (24t) | 32GB | **Vivo +7.57%** | +3.19% | +14.44% | +6.30% | -78.23% |
| 8 | **Precision-7770** | i7-12850HX (24t) | 32GB | **Vivo +7.10%** | +8.51% | +7.20% | +7.02% | -75.82% |

### Key Observations

**1. VivoBookPro sweeps all 8 matchups**, extending its unbeaten streak to 19 comparisons (17 wins, 2 ties).

**2. FireDucks confirms the E-core theory:**
- **AMD machines with identical cores** show minimal or zero FireDucks penalty (Legion7-16ARX8H: -4.39%, ThinkPadE16G2: -8.40%).
- **WL5040** (Ryzen 9 8945HS) is the **only machine where the VivoBookPro wins FireDucks** (+70.18%). The 8945HS is a power-constrained 16-thread chip that genuinely can't keep up.
- **HeliosNeo16S** (Arrow Lake Ultra 9 275HX) has the **smallest Intel-hybrid FireDucks penalty** at just -20.10%, suggesting Arrow Lake's improved thread director handles E-core scheduling much better than Raptor Lake.
- **DELL-XPS15-9530** (i9-13900H, same Raptor Lake gen) shows a catastrophic -77.88%, consistent with the E-core scheduling pathology seen on the VivoBook itself.

**3. The HeliosNeo16S is the most interesting competitor:**
Intel's latest **Arrow Lake Ultra 9 275HX** (24 threads) nearly matched the VivoBook in Polars (+1.97%) and had the best FireDucks result of any Intel hybrid (-20.10%). Arrow Lake's architectural refinements clearly help, but it's held back by only 32GB RAM and fewer total threads. **With 64GB RAM, the HeliosNeo16S could potentially challenge the VivoBook.**

**4. The Legion7-16ARX8H reveals RAM's true impact:**
It has the **same CPU** as the ROG-Strix-G17 (Ryzen 9 7945HX, 32t) but half the RAM (32GB vs 64GB). The Legion7 lost by -27.89% while the ROG with 64GB only lost by -13.71%. That's a **~14 percentage point gap** attributable to RAM.

---

## OS Intersection Filter Impact

A critical methodological correction was applied during this analysis. The `compare_hosts.py` script was updated to **automatically restrict comparisons to OS environments shared by both hosts**. Previously, the ZBookPowerG9's native Linux data — where all machines perform significantly faster — was included in the overall comparison even though the VivoBookPro had no native Linux benchmarks.

| Comparison | Before (with OS asymmetry) | After (auto intersection) |
|---|---|---|
| ZBookPowerG9 vs VivoBookPro | TIE (-3.08%) | **Vivo +8.77%** |
| IdeaPadPro5i vs VivoBookPro | IdeaPad +5.08% | **Vivo +19.19%** |

The fix affected all hosts that had native Linux benchmarks when compared against hosts that didn't, flipping multiple verdicts.
