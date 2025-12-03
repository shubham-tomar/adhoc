"""
Problem 8.1: Data Skew Resolution
==================================
Goal: Handle severely skewed data in joins and aggregations

Key Concepts:
- Skew detection using statistics
- Salting technique
- Adaptive Query Execution
- Performance comparison
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 8.1 - Data Skew Resolution") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 8.1: DATA SKEW RESOLUTION")
print("=" * 80)

# ============================================================================
# TASK 1: Create Skewed Dataset
# ============================================================================

print("\n" + "-" * 80)
print("TASK 1: CREATING SKEWED DATASET")
print("-" * 80)

"""
Skew Simulation:
- 80% of records belong to single key
- 20% distributed across other keys
- Realistic scenario (power users, hot products)
"""

# Create severely skewed data
skewed_data = []

# 80% goes to CUST0001 (hot key)
for i in range(8000):
    skewed_data.append(("CUST0001", f"TXN{i}", 100.0, "Electronics"))

# 20% distributed among others
for i in range(8000, 10000):
    skewed_data.append((f"CUST{i:04d}", f"TXN{i}", 100.0, "Books"))

df_skewed = spark.createDataFrame(
    skewed_data,
    ["customer_id", "txn_id", "amount", "category"]
)

print(f"Total records: {df_skewed.count()}")

# ============================================================================
# TASK 2: Identify Skew with Statistics
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: SKEW DETECTION")
print("=" * 80)

"""
Methods:
1. GroupBy count
2. Partition size distribution
3. Task duration variance
"""

# Method 1: GroupBy to find hot keys
print("\n1. Key Distribution Analysis:")
key_distribution = df_skewed.groupBy("customer_id").count() \
    .orderBy(col("count").desc())

print("\nTop 10 keys by count:")
key_distribution.show(10)

# Calculate skew metrics
stats = key_distribution.agg(
    max("count").alias("max_count"),
    min("count").alias("min_count"),
    avg("count").alias("avg_count"),
    stddev("count").alias("stddev_count")
).collect()[0]

skew_ratio = stats["max_count"] / stats["avg_count"]

print(f"\nSkew Metrics:")
print(f"  Max count: {stats['max_count']:,}")
print(f"  Avg count: {stats['avg_count']:,.0f}")
print(f"  Skew ratio: {skew_ratio:.2f}x")

if skew_ratio > 10:
    print("  ⚠️  SEVERE SKEW DETECTED!")
elif skew_ratio > 3:
    print("  ⚠️  Moderate skew detected")
else:
    print("  ✓ Acceptable distribution")

# Method 2: Partition distribution
print("\n2. Partition Size Distribution:")
df_partitioned_by_key = df_skewed.repartition(10, "customer_id")
partition_sizes = df_partitioned_by_key.rdd.glom().map(len).collect()

print(f"  Partition sizes: {partition_sizes}")
print(f"  Max: {max(partition_sizes)}, Min: {min(partition_sizes)}")
print(f"  Ratio: {max(partition_sizes)/min(partition_sizes):.2f}x")

# ============================================================================
# TASK 3: Baseline (Slow with Skew)
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: BASELINE PERFORMANCE (WITH SKEW)")
print("=" * 80)

# Customer dimension table
import random
customer_data = [(f"CUST{i:04d}", f"Customer_{i}", random.choice(["Premium", "Regular"]))
                 for i in range(1, 10001)]

df_customers = spark.createDataFrame(customer_data, ["customer_id", "name", "tier"])

print("\nPerforming join with skewed data...")

# Regular join (will have stragglers)
start = time.time()
df_joined_baseline = df_skewed.join(df_customers, "customer_id")
result_count = df_joined_baseline.count()
time_baseline = time.time() - start

print(f"  Records: {result_count:,}")
print(f"  Time: {time_baseline:.4f}s")
print("  ⚠️  Single partition processing 8000 records (straggler!)")

# ============================================================================
# TASK 4: AQE Skew Handling
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: ADAPTIVE QUERY EXECUTION (AQE)")
print("=" * 80)

"""
AQE automatically:
1. Detects skewed partitions
2. Splits large partition into smaller ones
3. Replicates matching data from other side
4. Processes in parallel
"""

print("\nWith AQE skewJoin enabled:")
print(f"  spark.sql.adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"  spark.sql.adaptive.skewJoin.enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

start = time.time()
df_joined_aqe = df_skewed.join(df_customers, "customer_id")
result_count = df_joined_aqe.count()
time_aqe = time.time() - start

print(f"\n  Records: {result_count:,}")
print(f"  Time: {time_aqe:.4f}s")
print(f"  Speedup: {time_baseline/time_aqe:.2f}x")

print("\nExecution plan (look for SkewJoin optimization):")
df_joined_aqe.explain()

# ============================================================================
# TASK 5: Manual Salting Technique
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: SALTING TECHNIQUE")
print("=" * 80)

"""
Salting Algorithm:
1. Add random salt (0-N) to skewed key
   "CUST0001" → "CUST0001_0", "CUST0001_1", ..., "CUST0001_9"

2. Replicate small table with all salts
   "CUST0001" → ["CUST0001_0", "CUST0001_1", ..., "CUST0001_9"]

3. Join on salted key
   - Distributes hot key across multiple partitions
   - No single slow task
"""

SALT_RANGE = 10  # Number of salts

print(f"\n1. Adding salt to large table (range: 0-{SALT_RANGE-1}):")

# Add random salt to skewed table
df_skewed_salted = df_skewed.withColumn(
    "salt",
    (rand() * SALT_RANGE).cast("int")
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))
)

print(f"   Sample salted keys:")
df_skewed_salted.select("customer_id", "salt", "customer_id_salted").show(5)

print(f"\n2. Replicating small table with all salts:")

# Replicate customers with all salts
df_customers_replicated = df_customers.withColumn(
    "salt",
    explode(array([lit(i) for i in range(SALT_RANGE)]))
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))
)

print(f"   Original customers: {df_customers.count()}")
print(f"   Replicated: {df_customers_replicated.count()} ({SALT_RANGE}x)")

print("\n3. Joining on salted key:")

start = time.time()
df_joined_salted = df_skewed_salted.join(
    df_customers_replicated,
    "customer_id_salted"
).drop("customer_id_salted", "salt")

result_count = df_joined_salted.count()
time_salted = time.time() - start

print(f"   Records: {result_count:,}")
print(f"   Time: {time_salted:.4f}s")
print(f"   Speedup vs baseline: {time_baseline/time_salted:.2f}x")

# Verify distribution
print("\n4. Verifying partition distribution:")
partition_sizes_salted = df_joined_salted.rdd.glom().map(len).collect()
print(f"   Partition sizes: {partition_sizes_salted}")
print(f"   Max/Min ratio: {max(partition_sizes_salted)/min(partition_sizes_salted):.2f}x")
print("   ✓ Much more balanced!")

# ============================================================================
# TASK 6: Broadcast Join Alternative
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: BROADCAST JOIN (When Applicable)")
print("=" * 80)

"""
If one side is small enough:
- Broadcast to all executors
- No shuffle needed
- Fastest option
"""

print("\nUsing broadcast join:")

start = time.time()
df_joined_broadcast = df_skewed.join(
    broadcast(df_customers),
    "customer_id"
)
result_count = df_joined_broadcast.count()
time_broadcast = time.time() - start

print(f"  Records: {result_count:,}")
print(f"  Time: {time_broadcast:.4f}s")
print(f"  Speedup: {time_baseline/time_broadcast:.2f}x")
print("  ✓ No shuffle, no skew issue!")

# ============================================================================
# TASK 7: Performance Comparison
# ============================================================================

print("\n" + "=" * 80)
print("TASK 7: PERFORMANCE COMPARISON")
print("=" * 80)

results = [
    ("Baseline (Skewed)", time_baseline, 1.0),
    ("AQE SkewJoin", time_aqe, time_baseline/time_aqe),
    ("Manual Salting", time_salted, time_baseline/time_salted),
    ("Broadcast Join", time_broadcast, time_baseline/time_broadcast)
]

print(f"\n{'Approach':<25} {'Time (s)':<12} {'Speedup':<10}")
print("-" * 50)
for approach, time_val, speedup in results:
    print(f"{approach:<25} {time_val:<12.4f} {speedup:<10.2f}x")

print("\nRecommendations:")
recommendations = """
1. Small dimension table:
   ✅ Use broadcast join (fastest)

2. Large-large join with skew:
   ✅ Enable AQE skewJoin (easiest)
   ✅ Manual salting for extreme skew

3. Aggregation with skew:
   ✅ Pre-aggregate with salting
   ✅ Increase partitions
   ✅ Use approximate aggregations
"""
print(recommendations)

# ============================================================================
# TASK 8: Skew in Aggregations
# ============================================================================

print("\n" + "=" * 80)
print("TASK 8: HANDLING SKEW IN AGGREGATIONS")
print("=" * 80)

"""
Skewed groupBy:
- Same techniques apply
- Salting for pre-aggregation
- Two-phase aggregation
"""

print("\n1. Skewed groupBy (baseline):")
start = time.time()
agg_baseline = df_skewed.groupBy("customer_id").agg(
    count("*").alias("txn_count"),
    sum("amount").alias("total_amount")
).count()
time_agg_baseline = time.time() - start

print(f"   Time: {time_agg_baseline:.4f}s")

print("\n2. Two-phase aggregation with salting:")

# Phase 1: Pre-aggregate with salt
df_pre_agg = df_skewed.withColumn(
    "salt",
    (rand() * SALT_RANGE).cast("int")
).groupBy("customer_id", "salt").agg(
    count("*").alias("txn_count_partial"),
    sum("amount").alias("total_amount_partial")
)

# Phase 2: Final aggregation without salt
start = time.time()
df_final_agg = df_pre_agg.groupBy("customer_id").agg(
    sum("txn_count_partial").alias("txn_count"),
    sum("total_amount_partial").alias("total_amount")
).count()
time_agg_salted = time.time() - start

print(f"   Time: {time_agg_salted:.4f}s")
print(f"   Speedup: {time_agg_baseline/time_agg_salted:.2f}x")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. SKEW DETECTION:
   ✓ GroupBy count to find hot keys
   ✓ Partition size distribution
   ✓ Task duration variance in Spark UI
   ✓ Skew ratio > 3 = problem

2. AQE SKEW HANDLING:
   ✓ Enable: spark.sql.adaptive.skewJoin.enabled
   ✓ Automatic detection and handling
   ✓ Easiest solution (Spark 3.0+)
   ✓ Works for joins

3. SALTING TECHNIQUE:
   ✓ Add random salt to hot keys
   ✓ Replicate small table with salts
   ✓ Join on salted key
   ✓ Works for joins and aggregations

4. BROADCAST JOIN:
   ✓ Best for small dimension tables
   ✓ No shuffle, no skew
   ✓ Use broadcast() hint

5. TWO-PHASE AGGREGATION:
   ✓ Pre-aggregate with salt
   ✓ Final aggregate without salt
   ✓ Distributes hot keys

6. CHOOSING STRATEGY:
   Small table → Broadcast
   AQE available → Enable skewJoin
   Extreme skew → Manual salting
   Aggregation → Two-phase

7. PREVENTION:
   ✓ Design keys to avoid concentration
   ✓ Partition by composite keys
   ✓ Monitor data distribution
   ✓ Alert on skew metrics
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Skew = Performance killer. Detect and fix!")
