"""
Problem 6.2: Shuffle Optimization
==================================
Goal: Optimize shuffle-heavy operations

Key Concepts:
- Shuffle partition tuning
- Adaptive Query Execution (AQE)
- Skewed join handling
- Shuffle metrics monitoring
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 6.2 - Shuffle Optimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 6.2: SHUFFLE OPTIMIZATION")
print("=" * 80)

df = spark.read.parquet("transactions.parquet")

# ============================================================================
# PART 1: Understanding Shuffle
# ============================================================================

print("\n" + "-" * 80)
print("PART 1: UNDERSTANDING SHUFFLE")
print("-" * 80)

"""
Shuffle: Data redistribution across partitions

Triggers:
- groupBy, aggregations
- join (except broadcast)
- distinct, repartition
- sortWithinPartitions

Cost:
- Disk writes (map side)
- Network transfer
- Disk reads (reduce side)
- Serialization/deserialization

Metrics:
- Shuffle write
- Shuffle read
- Shuffle spill (memory → disk)
"""

# Trigger shuffle
result = df.groupBy("category").agg(
    count("*").alias("count"),
    sum("amount").alias("total")
)

print(f"\nGroupBy result: {result.count()} categories")
print("\nPlan (look for Exchange = shuffle):")
result.explain()

# ============================================================================
# PART 2: Shuffle Partition Tuning
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: SHUFFLE PARTITION TUNING")
print("=" * 80)

"""
spark.sql.shuffle.partitions:
- Default: 200
- Controls partitions AFTER shuffle
- Critical for performance

Too Few:
- Large tasks (OOM)
- Underutilized cluster
- Stragglers

Too Many:
- Task overhead
- Many small files
- Slower

Sweet Spot:
- Partition size: 128MB-1GB
- Count: 2-4x cores
"""

# Test different shuffle partition counts
test_configs = [10, 50, 200, 500]

print("\nTesting groupBy with different shuffle partitions:")
for num_parts in test_configs:
    spark.conf.set("spark.sql.shuffle.partitions", str(num_parts))
    
    start = time.time()
    result = df.groupBy("category", "region").agg(
        sum("amount").alias("total")
    ).count()
    elapsed = time.time() - start
    
    print(f"  {num_parts:3d} partitions: {elapsed:.4f}s")

"""
Tuning Guidelines:

Data Size → Partitions:
- <1GB:     10-50
- 1-10GB:   50-200
- 10-100GB: 200-1000
- >100GB:   1000-5000

Formula:
partitions = data_size_GB / target_partition_size_GB
target = 0.2 (200MB per partition)
"""

# ============================================================================
# PART 3: Adaptive Query Execution (AQE)
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: ADAPTIVE QUERY EXECUTION")
print("=" * 80)

"""
AQE (Spark 3.0+):
- Dynamically optimizes at runtime
- Coalesces shuffle partitions
- Handles skewed joins
- Converts to broadcast joins

Enable:
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
"""

print("\nAQE Configuration:")
print(f"  adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"  coalescePartitions: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")
print(f"  skewJoin: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# Set high shuffle partitions (AQE will coalesce)
spark.conf.set("spark.sql.shuffle.partitions", "500")

print("\n1. Coalesce Partitions:")
print("   Initial: 500 shuffle partitions")

result = df.groupBy("category").agg(sum("amount").alias("total"))
final_parts = result.rdd.getNumPartitions()

print(f"   Final: {final_parts} partitions (AQE coalesced)")
print("   ✓ Automatically reduced from 500")

# ============================================================================
# PART 4: Skewed Join Optimization
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: HANDLING SKEWED JOINS")
print("=" * 80)

"""
Skewed Join Problem:
- One key has many records
- Goes to single partition
- Slow task (straggler)

AQE Solution:
- Detects skew automatically
- Splits large partition
- Processes in parallel
"""

# Create skewed data
print("\nCreating skewed dataset...")

skewed_data = []
for i in range(8000):
    skewed_data.append(("CUST0001", f"TXN{i}", 100.0))
for i in range(8000, 10000):
    skewed_data.append((f"CUST{i:04d}", f"TXN{i}", 100.0))

df_skewed = spark.createDataFrame(skewed_data, ["customer_id", "txn_id", "amount"])

# Customer dimension
import random
customer_data = [(f"CUST{i:04d}", f"Customer_{i}") for i in range(1, 101)]
df_customers = spark.createDataFrame(customer_data, ["customer_id", "customer_name"])

print("\nKey distribution:")
df_skewed.groupBy("customer_id").count().orderBy(col("count").desc()).show(5)

# Join with skew
print("\nJoining skewed data (AQE will handle):")
joined = df_skewed.join(df_customers, "customer_id")
print(f"  Result: {joined.count()} records")

print("\n  Plan (look for 'SkewJoin' optimization):")
joined.explain()

"""
AQE Skew Handling:

Detection:
- Partition size > threshold
- Threshold: 5x median partition size

Optimization:
- Split large partition
- Replicate matching data
- Process in parallel

Manual Alternative (Salting):
- Add random salt to skewed keys
- Replicate small table with salts
- Join on salted key
"""

# ============================================================================
# PART 5: Shuffle Metrics
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: MONITORING SHUFFLE METRICS")
print("=" * 80)

"""
Key Metrics (Spark UI):

Shuffle Write:
- Data written to disk (map side)
- Lower is better

Shuffle Read:
- Data read from disk (reduce side)
- Should equal shuffle write

Shuffle Spill:
- Memory overflow to disk
- Bad if high (increase memory)

Tasks:
- Task duration variance
- Detect stragglers

GC Time:
- High GC = memory pressure
- Increase executor memory
"""

print("\nShuffle Monitoring Checklist:")
checklist = """
□ Check Shuffle Write size in Spark UI
□ Verify Shuffle Read = Shuffle Write
□ Monitor spilled memory (should be low)
□ Look for task duration variance (skew)
□ Check GC time (< 10% of task time)
□ Verify partition count vs data size
□ Look for Exchange operators in plan
"""
print(checklist)

# ============================================================================
# PART 6: Optimization Strategies
# ============================================================================

print("\n" + "=" * 80)
print("PART 6: SHUFFLE OPTIMIZATION STRATEGIES")
print("=" * 80)

strategies = """
1. REDUCE SHUFFLE COUNT:
   ✓ Filter before groupBy/join
   ✓ Use broadcast joins for small tables
   ✓ Combine multiple aggregations
   
   Example:
   # Bad: Two shuffles
   df.groupBy("a").count()
   df.groupBy("a").sum("b")
   
   # Good: One shuffle
   df.groupBy("a").agg(count("*"), sum("b"))

2. TUNE PARTITION COUNT:
   ✓ Set spark.sql.shuffle.partitions
   ✓ Target: 128MB-1GB per partition
   ✓ Enable AQE for auto-tuning

3. HANDLE SKEW:
   ✓ Enable spark.sql.adaptive.skewJoin
   ✓ Manual salting for complex cases
   ✓ Broadcast if one side small

4. PRE-PARTITION DATA:
   ✓ Repartition by join key
   ✓ Cache partitioned data
   ✓ Reuse for multiple operations

5. MONITOR AND ITERATE:
   ✓ Check Spark UI metrics
   ✓ Profile slow queries
   ✓ Adjust based on data size
"""
print(strategies)

# Example: Combine aggregations
print("\nExample: Efficient aggregation")

# Bad: Multiple shuffles
print("❌ BAD (multiple shuffles):")
print("  df.groupBy('category').count()")
print("  df.groupBy('category').sum('amount')")
print("  → 2 shuffles!")

# Good: Single shuffle
print("\n✓ GOOD (single shuffle):")
efficient = df.groupBy("category").agg(
    count("*").alias("count"),
    sum("amount").alias("total"),
    avg("amount").alias("avg"),
    max("amount").alias("max")
)
print("  df.groupBy('category').agg(...)")
print("  → 1 shuffle!")

efficient.show()

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. SHUFFLE BASICS:
   ✓ Expensive operation (disk + network)
   ✓ Triggered by: groupBy, join, distinct, repartition
   ✓ Visible as "Exchange" in plan
   ✓ Monitor in Spark UI

2. PARTITION TUNING:
   ✓ spark.sql.shuffle.partitions
   ✓ Default 200 often not optimal
   ✓ Target: 128MB-1GB per partition
   ✓ Small data: 10-50, Large data: 500-2000

3. ADAPTIVE QUERY EXECUTION:
   ✓ Enable: spark.sql.adaptive.enabled=true
   ✓ Auto-coalesces partitions
   ✓ Handles skewed joins
   ✓ Converts to broadcast joins
   ✓ Spark 3.0+ feature

4. SKEW HANDLING:
   ✓ Enable AQE skewJoin
   ✓ Manual salting for extreme skew
   ✓ Monitor task duration variance
   ✓ Broadcast if possible

5. OPTIMIZATION TECHNIQUES:
   ✓ Filter early
   ✓ Broadcast small tables
   ✓ Combine aggregations
   ✓ Pre-partition data
   ✓ Cache reused DataFrames

6. MONITORING:
   ✓ Shuffle write/read size
   ✓ Spilled memory
   ✓ Task duration
   ✓ GC time
   ✓ Partition distribution

7. COMMON ISSUES:
   ⚠️  Too many partitions (overhead)
   ⚠️  Too few partitions (OOM)
   ⚠️  Data skew (stragglers)
   ⚠️  High shuffle spill (memory)
   ⚠️  Multiple shuffles (inefficient)
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Shuffle optimization = 10-100x performance improvement!")
