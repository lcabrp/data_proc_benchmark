# Analysis: HP-Z2-G9 (64GB) Performance

## Overview
This report evaluates the performance of the **HP-Z2-G9** workstation (featuring a *12th Gen Intel Core i9-12900*) when configured with 64GB of RAM. The analysis compares this configuration against its 128GB counterpart and other 64GB desktop models in the dataset.

> [!NOTE]
> For accuracy, physical RAM was estimated for WSL2 environments (which typically report ~50% of machine memory due to hypervisor limits). This ensures we were genuinely comparing the 64GB hardware against other 64GB hardware.

---

## 1. 64GB vs 128GB HP-Z2-G9

**Verdict:** The **64GB configuration is slightly faster (~6.64% overall)**.

The 128GB configuration does not yield a performance advantage for these 10M-row datasets. In fact, it slightly underperforms—a phenomenon sometimes observed due to looser quad-DIMM memory timings or dual-rank overhead associated with higher capacity setups on consumer/workstation platforms.
- **pandas:** Tie (Within 5% threshold)
- **polars:** 64GB model is ~14.89% faster
- **duckdb:** 64GB model is ~10.05% faster

---

## 2. 64GB HP-Z2-G9 vs Other 64GB Desktops

The 64GB HP-Z2-G9 (i9-12900) demonstrates elite performance, easily matching the absolute newest models and vastly outpacing older or lower-tier 64GB machines.

### The Competition (Statistical Ties)
These newer Dell OptiPlex models perform effectively identically to the HP-Z2-G9 (within the 5% margin of variance):
- **OptPlexMicro-7020** *(Intel Core i5-14600)*: Tie (OptPlex is ~0.26% faster overall)
- **OptPlex-7020-3** *(Intel Core i5-14600)*: Tie (HP-Z2-G9 is ~2.51% faster overall)

### Mid-Tier (HP-Z2-G9 Leads by 20-50%)
The Z2-G9 shows significant margins against mid-range systems, earlier revisions, and older generations:
- **LnvTCi5-12400 / Lenovo ThinkCentre** *(12th Gen Intel Core i5-12400)*: HP leads by ~23.66%
- **OptPlex-7010-1** *(13th Gen Intel Core i9-13900)*: HP leads by ~29.75%
- **LnvThkCtr-01** *(12th Gen Intel Core i7-12700T)*: HP leads by ~34.67%
- **TS-P350-01 / ThinkStation** *(11th Gen Intel Core i7-11700)*: HP leads by ~40.87%
- **OptPlex-7020-1** *(Intel Core i5-14500)*: HP leads by ~46.26%

### Entry-Tier (HP-Z2-G9 Dominates)
Against standard consumer desktops outfitted with 64GB RAM, the workstation architectural advantages are clearly visible:
- **Inspiron3030S** *(Intel Core i7-14700)*: HP leads by ~72.01% (and is almost 4x faster in `pandas` computations).

> [!TIP]
> **Takeaway:** The 64GB HP-Z2-G9 setup provides phenomenal, top-of-stack workstation performance. Since it outperforms the 128GB configuration on medium-size datasets (10M rows), the 64GB variant is the superior optimization choice *unless* future workloads strictly require in-memory footprints exceeding what a 64GB machine can provide.
