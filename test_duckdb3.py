import duckdb
import time

con = duckdb.connect()
print("Loading data into memory...")
con.execute("CREATE TABLE d AS SELECT * FROM read_parquet('data/raw/synthetic_logs_10M.parquet')")

q_orig = """
WITH summary AS (
    SELECT source_ip, SUM(bytes) AS total_bytes
    FROM d
    GROUP BY source_ip
),
ranked AS (
    SELECT d.*, s.total_bytes,
           DENSE_RANK() OVER (PARTITION BY d.event_type ORDER BY s.total_bytes DESC) AS total_rank
    FROM d
    JOIN summary s USING (source_ip)
)
SELECT * FROM ranked WHERE total_rank <= 10
"""

t0 = time.time()
r = con.execute(q_orig).fetch_arrow_table()
t1 = time.time()
print(f'fetch_arrow_table time: {t1-t0:.2f}s, rows: {r.num_rows}')

t2 = time.time()
df = r.to_pandas()
t3 = time.time()
print(f'arrow to pandas time: {t3-t2:.2f}s')
