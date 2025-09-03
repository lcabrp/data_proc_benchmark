"""
Small utility to convert a (possibly compressed) CSV/NDJSON log into Parquet
and report size savings. Uses pandas/pyarrow for maximum compatibility across
pandas, Modin, Polars, and DuckDB consumers.

Usage (PowerShell):
    # CSV -> Parquet (snappy)
    .venv/Scripts/python.exe scripts/tools/csv_to_parquet.py --input data/raw/logs.csv --out data/raw/logs.parquet

    # Compressed CSV -> Parquet (zstd)
    .venv/Scripts/python.exe scripts/tools/csv_to_parquet.py --input data/raw/logs.csv.gz --out data/raw/logs.parquet --compression zstd

    # NDJSON/JSONL -> Parquet
    .venv/Scripts/python.exe scripts/tools/csv_to_parquet.py --input data/raw/logs.ndjson --out data/raw/logs.parquet --format ndjson

Notes:
- Requires pyarrow. Install with: uv add pyarrow (or pip install pyarrow).
- For zstd compression, pyarrow wheel usually includes zstd on Windows.
- Supports chunked conversion to keep memory reasonable for 1.2GB+ files.
"""
from __future__ import annotations

import argparse
import pathlib
from typing import Optional

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit("pyarrow is required. Install with: uv add pyarrow or pip install pyarrow")

def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    x = float(n)
    while x >= 1024 and i < len(units) - 1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"

def infer_format(path: str) -> str:
    p = path.lower()
    if p.endswith(".ndjson") or p.endswith(".jsonl"):
        return "ndjson"
    return "csv"

def main() -> None:
    parser = argparse.ArgumentParser(description="Convert CSV/NDJSON to Parquet and report size savings")
    parser.add_argument("--input", required=True, help="Path to input file (.csv[.gz|.zst|...], .ndjson/.jsonl)")
    parser.add_argument("--out", required=True, help="Path to output Parquet file or directory (for partitioning)")
    parser.add_argument("--format", choices=["csv", "ndjson"], default=None, help="Force input format; default: infer by extension")
    parser.add_argument("--compression", choices=["snappy", "zstd", "gzip", "brotli", "none"], default="snappy", help="Parquet compression codec")
    parser.add_argument("--chunksize", type=int, default=1_000_000, help="Rows per chunk when converting (memory control)")
    parser.add_argument("--partition", default=None, help="Optional column name to partition by (writes directory)")
    args = parser.parse_args()

    input_path = pathlib.Path(args.input)
    out_path = pathlib.Path(args.out)
    fmt = args.format or infer_format(str(input_path))
    codec = None if args.compression == "none" else args.compression

    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Gather original size (on-disk of compressed source if compressed)
    src_size = input_path.stat().st_size

    # Writer setup
    writer: Optional[pq.ParquetWriter] = None
    rows_total = 0
    chunks = 0

    try:
        if fmt == "csv":
            reader = pd.read_csv(input_path, chunksize=args.chunksize, low_memory=False)
        else:  # ndjson
            reader = pd.read_json(input_path, lines=True, chunksize=args.chunksize)

        for chunk_df in reader:
            # Normalize dtypes a bit: let pyarrow infer, but ensure timestamps parse if present
            table = pa.Table.from_pandas(chunk_df, preserve_index=False)

            if args.partition:
                # Partitioned write: slower, but scales; creates a directory tree
                pq.write_to_dataset(
                    table,
                    root_path=str(out_path),
                    partition_cols=[args.partition],
                    compression=codec or "snappy",
                    existing_data_behavior="overwrite_or_ignore",
                )
            else:
                if writer is None:
                    writer = pq.ParquetWriter(str(out_path), table.schema, compression=codec or "snappy")
                if writer is not None:
                    writer.write_table(table)

            rows_total += len(chunk_df)
            chunks += 1
    finally:
        if writer is not None:
            writer.close()

    # Compute new size (file or directory)
    if out_path.is_dir():
        dest_size = sum(p.stat().st_size for p in out_path.rglob("*.parquet"))
    else:
        dest_size = out_path.stat().st_size

    ratio = (1 - (dest_size / src_size)) if src_size > 0 else 0

    print("Conversion complete:")
    print(f" - Source: {input_path} ({human_bytes(src_size)})")
    if out_path.is_dir():
        print(f" - Parquet dir: {out_path} ({human_bytes(dest_size)})")
    else:
        print(f" - Parquet file: {out_path} ({human_bytes(dest_size)})")
    print(f" - Rows written: {rows_total:,} in {chunks} chunks")
    print(f" - Compression: {args.compression}")
    print(f" - Size reduction: {ratio*100:.1f}%")

if __name__ == "__main__":
    main()
