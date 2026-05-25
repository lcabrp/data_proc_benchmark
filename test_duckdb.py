import duckdb
import time

con = duckdb.connect()
print("Loading data into memory...")
con.execute("CREATE TABLE d AS SELECT * FROM read_parquet('data/raw/synthetic_logs_10M.parquet')")
print("Data loaded. Running QUALIFY query...")

q_qualify = """
WITH summary AS (
    SELECT source_ip, SUM(bytes) AS total_bytes
    FROM d
    GROUP BY source_ip
)
SELECT d.*, s.total_bytes,
       DENSE_RANK() OVER (PARTITION BY d.event_type ORDER BY s.total_bytes DESC) AS total_rank
FROM d
JOIN summary s USING (source_ip)
QUALIFY total_rank <= 10
"""

t0 = time.time()
r = con.execute(q_qualify).fetchdf()
t1 = time.time()
print(f'Qualify time: {t1-t0:.2f}s, rows: {len(r)}')
