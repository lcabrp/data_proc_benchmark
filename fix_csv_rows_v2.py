import csv
import argparse
from pathlib import Path
from typing import List, Optional, Iterable

def fix_csv_structure(file_path: str) -> int:
    """
    Fixes the structure of a CSV file by ensuring all rows have 
    the same length as the header row. Uses a streaming approach 
    to handle large files and an atomic-style write to ensure data safety.

    Args:
        file_path: Path to the CSV file to repair.

    Returns:
        int: The number of rows that were modified.

    Raises:
        FileNotFoundError: If the provided path does not exist.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"The file at {path} was not found.")

    # Step 1: Determine target length from header
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            print(f"Warning: File {path} is empty.")
            return 0
        target_len = len(header)

    # Step 2: Process and write to a temporary file
    # Using a .tmp suffix ensures we don't leave a broken file if the process is interrupted.
    temp_path = path.with_suffix(".tmp")
    fixed_count = 0

    try:
        with open(path, "r", encoding="utf-8", newline="") as fin:
            # We need to re-initialize a reader because we want to process the whole file
            # but standard practice for modification is reading from one and writing to another.
            fin_reader = csv.reader(fin)
            
            # Re-read header since we are opening a new stream for the write operation
            actual_header = next(fin_reader, None)
            if actual_header is None:
                return 0
            
            target_len = len(actual_header)

            with open(temp_path, "w", encoding="utf-8", newline="") as fout:
                writer = csv.writer(fout)
                writer.writerow(actual_header)

                for row in fin_reader:
                    current_len = len(row)
                    if current_len != target_len:
                        fixed_count += 1
                        if current_len < target_len:
                            # Pad with empty strings
                            row.extend([""] * (target_len - current_len))
                        else:
                            # Truncate
                            row = row[:target_len]
                    writer.writerow(row)

        # Step 3: Atomic replacement
        # replace() is safer than move/rename because it handles some OS-specific edge cases.
        temp_path.replace(path)
    except Exception as e:
        if temp_path.exists():
            temp_path.unlink()  # Clean up the temp file if something goes wrong
        raise e

    return fixed_count

def main():
    parser = argparse.ArgumentParser(
        description="Fixes CSV structure by aligning row lengths to match the header."
    )
    parser.add_argument(
        "file", 
        help="Path to the CSV file to be fixed"
    )
    args = parser.parse_args()

    try:
        count = fix_csv_structure(args.file)
        if count > 0:
            print(f"Success: Fixed {count} rows in '{args.file}'.")
        else:
            print(f"Success: No issues found in '{args.file}'.")
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()