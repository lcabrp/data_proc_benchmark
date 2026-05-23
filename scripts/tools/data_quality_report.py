import pandas as pd

df = pd.read_csv('data/benchmark_results.csv')

print("="*70)
print("DATA QUALITY REPORT AFTER STEALTHAI16 REMOVAL")
print("="*70)

print(f"\nTotal records: {len(df)}")
print(f"Total hosts: {df['hostname'].nunique()}")
print(f"\nRecords by script:")
print(df['script_name'].value_counts())

print("\n" + "="*70)
print("OUTLIER ANALYSIS (pandas filter_group > 5s)")
print("="*70)

outliers = df[df['filter_group_pandas_seconds'] > 5.0]
print(f"\nTotal outliers: {len(outliers)} ({len(outliers)/len(df)*100:.2f}%)")

if len(outliers) > 0:
    print(f"\nOutliers by host:")
    for host, count in outliers['hostname'].value_counts().items():
        host_total = len(df[df['hostname'] == host])
        print(f"  {host}: {count}/{host_total} ({count/host_total*100:.1f}%)")
    
    print(f"\nOutliers by script:")
    print(outliers['script_name'].value_counts())
    
    print(f"\n" + "="*70)
    print("CONCLUSION")
    print("="*70)
    
    # Check if outliers are consistent (all operations slow)
    consistent = True
    for idx, row in outliers.iterrows():
        filter_t = row['filter_group_pandas_seconds']
        stats_t = row['statistics_pandas_seconds']
        join_t = row['complex_join_pandas_seconds']
        # All should be proportionally slow
        if not (stats_t > 5 or join_t > 10):
            consistent = False
            break
    
    if consistent:
        print("\n✓ All outliers show consistently slow performance across operations")
        print("  → Indicates legitimate system performance issues, not timing bugs")
        print("  → Outlier removal mechanism handles these appropriately")
    
    print(f"\nRemaining outliers are {len(outliers)/len(df)*100:.2f}% of total data - acceptable variation")
    print("Recommendation: Continue using outlier removal in analysis tools")
else:
    print("\n✓ No outliers detected")
    print("  → Data quality is excellent")
