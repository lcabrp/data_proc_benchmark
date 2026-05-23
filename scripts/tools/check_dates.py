import pandas as pd

df = pd.read_csv('data/benchmark_results.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

print('Test dates by hostname:\n')
for host in sorted(df['hostname'].unique()):
    host_df = df[df['hostname'] == host]
    print(f'{host}:')
    print(f'  Date range: {host_df["timestamp"].min()} to {host_df["timestamp"].max()}')
    print(f'  Total records: {len(host_df)}')
    outlier_count = len(host_df[host_df['filter_group_pandas_seconds'] > 5.0])
    if outlier_count > 0:
        print(f'  Outliers: {outlier_count}')
    print()

print('\n' + '='*60)
print('StealthAI16 detailed analysis:')
print('='*60)
stealth = df[df['hostname'] == 'StealthAI16'].sort_values('timestamp')
print(f'Total runs: {len(stealth)}')
print(f'Date range: {stealth["timestamp"].min()} to {stealth["timestamp"].max()}')

outliers_stealth = stealth[stealth['filter_group_pandas_seconds'] > 5.0]
print(f'\nOutliers: {len(outliers_stealth)}')
if len(outliers_stealth) > 0:
    print(f'Outlier dates: {outliers_stealth["timestamp"].min()} to {outliers_stealth["timestamp"].max()}')
    print('\nOutlier records:')
    print(outliers_stealth[['timestamp', 'script_name', 'filter_group_pandas_seconds']].to_string())
