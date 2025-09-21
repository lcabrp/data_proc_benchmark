
REM Create 10 million synthetic log records and save as CSV
.venv\Scripts\python.exe scripts\log-gen\test_generator_01.py --output data\raw\synthetic_logs_10M.csv --rows 10000000

REM Convert the generated CSV to Parquet format with Zstandard compression
.venv\Scripts\python.exe scripts\tools\csv_to_parquet.py --input data\raw\synthetic_logs_10M.csv --out data\raw\synthetic_logs_10M.parquet --compression zstd