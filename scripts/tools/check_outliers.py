import pandas as pd

df = pd.read_csv('data/benchmark_results.csv')
outliers = df[df['filter_group_pandas_seconds'] > 5.0].copy()

print('When filter_group is slow, are other operations also slow?\n')
for idx, row in outliers.head(10).iterrows():
    print(f'Row {idx}: {row["hostname"]} - {row["script_name"]}')
    print(f'  filter: {row["filter_group_pandas_seconds"]:.2f}s')
    print(f'  stats: {row["statistics_pandas_seconds"]:.2f}s')
    print(f'  join: {row["complex_join_pandas_seconds"]:.2f}s')
    print(f'  timeseries: {row["timeseries_pandas_seconds"]:.2f}s')
    print()
