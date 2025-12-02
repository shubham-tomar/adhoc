"""
Problem 4.1: Understanding Partitioning
========================================
Goal: Explore how data is distributed across partitions

Key Concepts:
- Partition inspection techniques
- Data distribution analysis
- Partition size monitoring
- Skew detection
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 4.1 - Understanding Partitioning") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 4.1: UNDERSTANDING PARTITIONING")
print("=" * 80)

# Read data
df = spark.read.csv("transactions.csv", header=True, inferSchema=True)

print(f"\nDataset: {df.count()} records")

# ============================================================================
# PART 1: Check Initial Partition Count
# ============================================================================

print("\n" + "-" * 80)
print("PART 1: INITIAL PARTITION COUNT")
print("-" * 80)

"""
Ways to check partition count:

1. df.rdd.getNumPartitions() - Most common
2. df.rdd.getNumPartitions
3. Check physical plan
"""

partition_count = df.rdd.getNumPartitions()
print(f"\nInitial partition count: {partition_count}")

"""
Learning Point: Default Partition Count

When reading files:
- CSV/JSON: Number of files or file splits
- Parquet: Number of row groups/files
- Default split size: spark.sql.files.maxPartitionBytes (128MB)

Formula: num_partitions ≈ total_size / 128MB
"""

# Check Spark configuration
print("\nRelevant Spark Configurations:")
print(f"  spark.sql.files.maxPartitionBytes: {spark.conf.get('spark.sql.files.maxPartitionBytes', 'default')}")
print(f"  spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  spark.default.parallelism: {spark.sparkContext.defaultParallelism}")

# ============================================================================
# PART 2: Inspect Partition Sizes
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: PARTITION SIZE INSPECTION")
print("=" * 80)

"""
Technique: Use rdd.glom() to inspect partition contents

glom() - Collects each partition into an array
map(len) - Counts rows in each partition
collect() - Brings counts to driver
"""

def analyze_partitions(df, name="DataFrame"):
    """Comprehensive partition analysis"""
    partition_sizes = df.rdd.glom().map(len).collect()
    
    print(f"\n{name}:")
    print(f"  {'Metric':<30} {'Value':<15}")
    print(f"  {'-'*30} {'-'*15}")
    print(f"  {'Total Partitions':<30} {len(partition_sizes):<15}")
    print(f"  {'Total Records':<30} {sum(partition_sizes):<15,}")
    print(f"  {'Min Partition Size':<30} {min(partition_sizes):<15,}")
    print(f"  {'Max Partition Size':<30} {max(partition_sizes):<15,}")
    print(f"  {'Avg Partition Size':<30} {sum(partition_sizes)/len(partition_sizes):<15,.1f}")
    print(f"  {'Median Partition Size':<30} {sorted(partition_sizes)[len(partition_sizes)//2]:<15,}")
    
    # Calculate skew
    avg_size = sum(partition_sizes) / len(partition_sizes)
    max_size = max(partition_sizes)
    skew_ratio = max_size / avg_size if avg_size > 0 else 0
    
    print(f"  {'Skew Ratio (max/avg)':<30} {skew_ratio:<15,.2f}")
    
    if skew_ratio > 3:
        print(f"  {'⚠️  DATA SKEW DETECTED!':<30}")
    else:
        print(f"  {'✓ Balanced Distribution':<30}")
    
    # Show distribution
    print(f"\n  Partition Size Distribution (first 20):")
    for i, size in enumerate(partition_sizes[:20]):
        bar = '█' * int(size / max(partition_sizes) * 50)
        print(f"    Partition {i:2d}: {size:6,} rows {bar}")
    
    if len(partition_sizes) > 20:
        print(f"    ... and {len(partition_sizes) - 20} more partitions")
    
    return partition_sizes

# Analyze initial distribution
partition_sizes = analyze_partitions(df, "Initial DataFrame")

"""
Ideal Partition Characteristics:

Size:
✓ 128MB - 1GB per partition
✓ Balance between parallelism and overhead
✗ Too small: Task scheduling overhead
✗ Too large: Memory pressure, stragglers

Distribution:
✓ Even size across partitions
✓ Skew ratio < 2
✗ Skew ratio > 3: Performance problems

Count:
✓ 2-4x number of cores for optimal parallelism
✓ More partitions = more parallelism (to a point)
✗ Too many: Overhead
✗ Too few: Underutilized cluster
"""

# ============================================================================
# PART 3: Repartition to Different Counts
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: REPARTITIONING")
print("=" * 80)

"""
repartition(n):
- Performs full shuffle
- Redistributes data evenly
- Can increase or decrease partitions
- Expensive but creates balanced partitions
"""

print("\n--- Repartition to 20 Partitions ---")
df_repart_20 = df.repartition(20)
print(f"New partition count: {df_repart_20.rdd.getNumPartitions()}")

sizes_20 = analyze_partitions(df_repart_20, "After repartition(20)")

"""
Observations:
- Full shuffle occurred (expensive!)
- Data redistributed evenly
- Each partition has roughly equal rows
- Good for: Increasing parallelism before heavy operations
"""

# ============================================================================
# PART 4: Repartition by Column
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: REPARTITION BY COLUMN")
print("=" * 80)

"""
repartition(n, col):
- Hash partitions by column values
- All records with same key go to same partition
- Useful before groupBy/join on that column
- Can create skew if key distribution is uneven
"""

print("\n--- Repartition by 'region' (4 unique values) ---")

# Check unique regions
unique_regions = df.select("region").distinct().count()
print(f"Unique regions: {unique_regions}")

# Repartition by region
df_by_region = df.repartition(10, "region")
print(f"Partition count: {df_by_region.rdd.getNumPartitions()}")

sizes_region = analyze_partitions(df_by_region, "After repartition(10, 'region')")

"""
Repartition by Column Effects:

Benefits:
✓ Co-locates data with same key
✓ Faster subsequent groupBy('region')
✓ Faster joins on 'region'

Risks:
⚠️  Skew if key distribution uneven
⚠️  May not use all partitions (only 4 regions, 10 partitions)
⚠️  Full shuffle cost

Check distribution by key:
"""

print("\n  Records per Region:")
df.groupBy("region").count().show()

# ============================================================================
# PART 5: Coalesce vs Repartition
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: COALESCE vs REPARTITION")
print("=" * 80)

"""
coalesce(n):
- Reduces partitions WITHOUT full shuffle
- Combines adjacent partitions
- FAST but may create skew
- Can only reduce, not increase

repartition(n):
- Full shuffle to redistribute
- Can increase or decrease
- SLOW but balanced
- Always creates even distribution
"""

# Start with many partitions
df_many = df.repartition(50)
print(f"\nStarting with {df_many.rdd.getNumPartitions()} partitions")

# Method 1: coalesce (no full shuffle)
print("\n" + "-" * 80)
print("COALESCE: Reduce without full shuffle")
print("-" * 80)

start = time.time()
df_coalesced = df_many.coalesce(5)
df_coalesced.count()  # Trigger execution
time_coalesce = time.time() - start

print(f"Time: {time_coalesce:.4f}s")
sizes_coalesce = analyze_partitions(df_coalesced, "After coalesce(5)")

# Method 2: repartition (full shuffle)
print("\n" + "-" * 80)
print("REPARTITION: Full shuffle")
print("-" * 80)

start = time.time()
df_repartitioned = df_many.repartition(5)
df_repartitioned.count()  # Trigger execution
time_repart = time.time() - start

print(f"Time: {time_repart:.4f}s")
sizes_repart = analyze_partitions(df_repartitioned, "After repartition(5)")

# Compare
print("\n" + "-" * 80)
print("COMPARISON")
print("-" * 80)

print(f"\n{'Operation':<20} {'Time':<15} {'Max Partition':<15} {'Min Partition':<15} {'Skew Ratio':<15}")
print("-" * 80)

def calc_skew(sizes):
    avg = sum(sizes) / len(sizes)
    return max(sizes) / avg if avg > 0 else 0

print(f"{'coalesce(5)':<20} {time_coalesce:<15.4f} {max(sizes_coalesce):<15,} {min(sizes_coalesce):<15,} {calc_skew(sizes_coalesce):<15.2f}")
print(f"{'repartition(5)':<20} {time_repart:<15.4f} {max(sizes_repart):<15,} {min(sizes_repart):<15,} {calc_skew(sizes_repart):<15.2f}")

print(f"\nSpeed difference: coalesce is {time_repart/time_coalesce:.2f}x faster")
print(f"Balance trade-off: repartition creates more balanced partitions")

"""
When to Use Each:

COALESCE:
✓ Reducing partitions (many → few)
✓ Before writing output (fewer files)
✓ Speed is priority
✓ Some skew acceptable
✗ DON'T use to increase partitions (does nothing)

Example: coalesce(1) before writing single output file

REPARTITION:
✓ Increasing partitions (few → many)
✓ Fixing skewed data
✓ Need balanced distribution
✓ Before expensive operations (join, groupBy)
✗ Expensive shuffle cost

Example: repartition(200) before large join
"""

# ============================================================================
# PART 6: Monitoring Partition Performance
# ============================================================================

print("\n" + "=" * 80)
print("PART 6: PARTITION PERFORMANCE IMPACT")
print("=" * 80)

"""
Test how partition count affects performance
"""

print("\nTesting groupBy performance with different partition counts...")

test_configs = [
    (1, "Single partition"),
    (5, "5 partitions"),
    (10, "10 partitions"),
    (50, "50 partitions"),
    (200, "200 partitions")
]

results = []

for num_parts, desc in test_configs:
    # Repartition
    df_test = df.repartition(num_parts)
    
    # Perform groupBy (shuffle operation)
    start = time.time()
    result = df_test.groupBy("category").agg(
        count("*").alias("count"),
        sum("amount").alias("total")
    ).count()
    elapsed = time.time() - start
    
    results.append((num_parts, desc, elapsed))
    print(f"  {desc:<20}: {elapsed:.4f}s")

# Find optimal
optimal = min(results, key=lambda x: x[2])
print(f"\n  ✓ Optimal: {optimal[1]} ({optimal[2]:.4f}s)")

"""
Performance Pattern:

Too Few Partitions (1-5):
- Underutilized cluster
- Slow processing
- May OOM on large data

Sweet Spot (10-50):
- Good parallelism
- Efficient task scheduling
- Best performance

Too Many Partitions (>200):
- Task scheduling overhead
- Many small tasks
- Diminishing returns

Rule of Thumb:
- Partitions = 2-4x number of cores
- Partition size = 128MB - 1GB
- Adjust based on data size
"""

# ============================================================================
# PART 7: Detecting and Visualizing Skew
# ============================================================================

print("\n" + "=" * 80)
print("PART 7: SKEW DETECTION")
print("=" * 80)

"""
Data Skew: When partitions have significantly different sizes
Impact: Slowest partition determines job completion time
"""

# Create intentionally skewed data
print("\nCreating skewed dataset for demonstration...")

from pyspark.sql.types import *
skewed_data = []
# 80% of records go to one customer
for i in range(8000):
    skewed_data.append(("CUST0001", f"TXN{i}", 100.0))
# 20% distributed among others
for i in range(8000, 10000):
    skewed_data.append((f"CUST{i:04d}", f"TXN{i}", 100.0))

df_skewed = spark.createDataFrame(skewed_data, ["customer_id", "txn_id", "amount"])

# Partition by customer_id (will be skewed)
df_skewed_part = df_skewed.repartition(10, "customer_id")

print("\nSkewed Distribution:")
sizes_skewed = analyze_partitions(df_skewed_part, "Skewed DataFrame")

"""
Skew Detection Metrics:

Coefficient of Variation (CV):
CV = stddev / mean
✓ CV < 0.5: Well balanced
⚠️  CV > 1.0: Moderate skew
❌ CV > 2.0: Severe skew

Skew Ratio:
Ratio = max_partition / avg_partition
✓ Ratio < 2: Good
⚠️  Ratio 2-3: Moderate skew
❌ Ratio > 3: Severe skew

Impact:
- Stragglers (slow tasks)
- OOM errors
- Underutilized cluster
"""

# Calculate statistics
import statistics
mean_size = statistics.mean(sizes_skewed)
std_size = statistics.stdev(sizes_skewed) if len(sizes_skewed) > 1 else 0
cv = std_size / mean_size if mean_size > 0 else 0

print(f"\nSkew Metrics:")
print(f"  Mean: {mean_size:,.1f}")
print(f"  Std Dev: {std_size:,.1f}")
print(f"  Coefficient of Variation: {cv:.2f}")
print(f"  Skew Ratio: {max(sizes_skewed) / mean_size:.2f}")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. PARTITION INSPECTION:
   ✓ df.rdd.getNumPartitions() - Count partitions
   ✓ df.rdd.glom().map(len).collect() - Size per partition
   ✓ Monitor for skew (max/avg ratio)

2. PARTITION SIZING:
   ✓ Ideal: 128MB - 1GB per partition
   ✓ Too small: Overhead dominates
   ✓ Too large: Memory pressure, stragglers
   ✓ Count: 2-4x number of cores

3. REPARTITION vs COALESCE:
   ✓ repartition(): Full shuffle, balanced, can increase/decrease
   ✓ coalesce(): Partial shuffle, fast, only decrease
   ✓ Use coalesce before writing to reduce files
   ✓ Use repartition to fix skew or increase parallelism

4. REPARTITION BY COLUMN:
   ✓ Co-locates data with same key
   ✓ Faster subsequent operations on that key
   ⚠️  Can create skew if distribution uneven

5. SKEW DETECTION:
   ✓ Skew ratio > 3: Problem
   ✓ Check partition size distribution
   ✓ Monitor task duration in Spark UI
   ✓ Fix with: repartition, salting, broadcast

6. PERFORMANCE TUNING:
   ✓ Adjust spark.sql.shuffle.partitions
   ✓ Repartition before expensive operations
   ✓ Coalesce before writing
   ✓ Balance between parallelism and overhead

7. MONITORING TOOLS:
   ✓ Spark UI: Task metrics, skew visualization
   ✓ explain(): See shuffle operations
   ✓ rdd.glom(): Inspect partition contents
   ✓ Metrics: partition count, size distribution

8. ANTI-PATTERNS:
   ✗ Too many small partitions
   ✗ Repartitioning multiple times
   ✗ Ignoring data skew
   ✗ Not checking partition distribution
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Always check Spark UI (localhost:4040) for partition metrics!")
