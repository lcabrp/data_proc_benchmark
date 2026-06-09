"""Backfill optimize-mode labels into historical benchmark ``script_name`` values.

Background
----------
Only ``benchmark.py`` historically encoded its memory-optimization mode into the
``script_name`` column (e.g. ``benchmark.py_opt_always``). The sibling runners
``benchmark_01.py``, ``benchmark_02.py`` and ``benchmark_modular.py`` wrote a bare
filename with no suffix, even though the ``benchmark_all_*`` driver ran them with
``-opt always``. That labelling gap was fixed going forward; this script backfills
the *existing* rows so the whole CSV is consistent and filterable by optimize mode.

Scope & safety
--------------
- Only rows whose ``script_name`` is EXACTLY one of the bare target names are touched
  (``benchmark_01.py``, ``benchmark_02.py``, ``benchmark_modular.py``). Rows that
  already carry a suffix (any ``*_opt_*`` / ``*_no_opt_*`` value) are left untouched.
- Idempotent: after a run the targets are renamed, so re-running changes nothing.
- A timestamped backup of the CSV is written before any modification (unless
  ``--no-backup``). Use ``--dry-run`` to preview counts without writing.
- Values are passed through verbatim via the ``csv`` module; no numeric reformatting.

Caveat
------
This assumes every bare 01/02/modular row was produced with ``-opt always``. The
project owner confirmed this was the case "most of the time"; there is no longer a
reliable per-row signal to distinguish the rare exceptions, so all matching rows are
labelled ``opt_always``.

Usage
-----
    python scripts/tools/backfill_script_name_opt_always.py --dry-run
    python scripts/tools/backfill_script_name_opt_always.py
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV = PROJECT_ROOT / "data" / "benchmark_results.csv"

# Bare script names to upgrade, mapped to their backfilled value.
TARGET_SUFFIX = "_opt_always"
TARGET_NAMES = (
    "benchmark_01.py",
    "benchmark_02.py",
    "benchmark_modular.py",
)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help=f"Results CSV path (default: {DEFAULT_CSV})")
    parser.add_argument("--dry-run", action="store_true", help="Report what would change without writing.")
    parser.add_argument("--no-backup", action="store_true", help="Skip writing a timestamped backup before modifying.")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    csv_path: Path = args.csv

    if not csv_path.exists():
        print(f"Error: CSV not found: {csv_path}")
        return 2

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if "script_name" not in fieldnames:
        print("Error: 'script_name' column not present in CSV; nothing to do.")
        return 2

    targets = set(TARGET_NAMES)
    before = Counter(r.get("script_name") for r in rows)
    to_change = sum(before.get(name, 0) for name in TARGET_NAMES)

    print(f"CSV: {csv_path}")
    print(f"Total rows: {len(rows)}")
    print("Rows matching bare target names:")
    for name in TARGET_NAMES:
        print(f"  {before.get(name, 0):4}  {name!r} -> {name + TARGET_SUFFIX!r}")
    print(f"Total to backfill: {to_change}")

    if to_change == 0:
        print("Nothing to backfill (already consistent). No changes made.")
        return 0

    if args.dry_run:
        print("\n[dry-run] No files written.")
        return 0

    if not args.no_backup:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = csv_path.with_name(f"{csv_path.stem}.backup_{stamp}{csv_path.suffix}")
        shutil.copy2(csv_path, backup_path)
        print(f"\nBackup written: {backup_path}")

    changed = 0
    for r in rows:
        name = r.get("script_name")
        if name in targets:
            r["script_name"] = name + TARGET_SUFFIX
            changed += 1

    # Write atomically: temp file in same dir, then replace.
    tmp_path = csv_path.with_name(csv_path.name + ".tmp")
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp_path.replace(csv_path)

    after = Counter(r.get("script_name") for r in rows)
    print(f"Backfilled {changed} rows.")
    print("Post-change counts for affected labels:")
    for name in TARGET_NAMES:
        new_name = name + TARGET_SUFFIX
        print(f"  {name!r}: {after.get(name, 0)} | {new_name!r}: {after.get(new_name, 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
