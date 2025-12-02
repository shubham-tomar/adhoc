"""
Problem 2.2: Transformation Types - Narrow vs Wide
===================================================
Goal: Understand narrow and wide transformations and their performance impact

Key Concepts:
- Narrow transformations: No shuffle (data stays in same partition)
- Wide transformations: Require shuffle (data moves across partitions)
- Shuffle is expensive (disk I/O + network transfer)
- Partition management and shuffle partition tuning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 2.2 - Transformation Types") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 2.2: NARROW vs WIDE TRANSFORMATIONS")
print("=" * 80)

# ============================================================================
# PART 1: NARROW TRANSFORMATIONS (No Shuffle)
# ============================================================================

print("\n" + "-" * 80)
print("PART 1: NARROW TRANSFORMATIONS")
print("-" * 80)

"""
NARROW Transformations:
- Each input partition → exactly ONE output partition
- No data movement between executors
- Process data locally on each partition
- FAST (no network/disk overhead)

Examples:
- filter()     : Filter rows in each partition
- select()     : Select columns in each partition
- map()        : Transform each row
- flatMap()    : Transform + flatten
- withColumn() : Add/modify column
- drop()       : Remove columns
- union()      : Combine DataFrames (if same partitioning)
"""

# Read data
df = spark.read.csv("transactions.csv", header=True, inferSchema=True)

print(f"\nInitial DataFrame:")
print(f"  Partitions: {df.rdd.getNumPartitions()}")
print(f"  Records: {df.count()}")

# Narrow Transformation 1: filter()
print("\n1. FILTER (Narrow)")
print("   Each partition filters its own data independently")
start = time.time()
df_filtered = df.filter(col("amount") > 100)
result_count = df_filtered.count()
elapsed = time.time() - start

print(f"   Partitions before: {df.rdd.getNumPartitions()}")
print(f"   Partitions after:  {df_filtered.rdd.getNumPartitions()}")
print(f"   Records after filter: {result_count}")
print(f"   Time: {elapsed:.4f}s")
print("   ✓ Partition count unchanged (no shuffle)")

# Narrow Transformation 2: map (using withColumn)
print("\n2. MAP/withColumn (Narrow)")
print("   Transform each row in its partition")
start = time.time()
df_mapped = df_filtered.withColumn("amount_usd", col("amount") * 1.0) \
                       .withColumn("amount_category", 
                                  when(col("amount") > 500, "High")
                                  .when(col("amount") > 100, "Medium")
                                  .otherwise("Low"))
result_count = df_mapped.count()
elapsed = time.time() - start

print(f"   Partitions: {df_mapped.rdd.getNumPartitions()}")
print(f"   Records: {result_count}")
print(f"   Time: {elapsed:.4f}s")
print("   ✓ No shuffle, just row-by-row transformation")

# Narrow Transformation 3: select
print("\n3. SELECT (Narrow)")
print("   Project columns from each partition")
df_selected = df_mapped.select("customer_id", "amount", "amount_category")
print(f"   Partitions: {df_selected.rdd.getNumPartitions()}")
print("   ✓ Column pruning, no shuffle needed")

# Narrow Transformation 4: flatMap (using explode)
print("\n4. FLATMAP (Narrow - with array explosion)")
print("   Example: Split comma-separated values")
df_with_array = df.withColumn("tags", array(lit("tag1"), lit("tag2"), lit("tag3")))
df_exploded = df_with_array.select("transaction_id", explode("tags").alias("tag"))
print(f"   Original partitions: {df_with_array.rdd.getNumPartitions()}")
print(f"   After explode: {df_exploded.rdd.getNumPartitions()}")
print(f"   ✓ Each partition explodes its own rows")

# Check execution plan for narrow operations
print("\n5. Execution Plan (Narrow Operations):")
df_selected.explain()

"""
Learning Points - Narrow Transformations:

Performance Characteristics:
✓ FAST: Process in-memory on each partition
✓ No network transfer
✓ No disk I/O (unless memory pressure)
✓ Linearly scalable (more partitions = more parallelism)

Partition Count:
✓ Remains same as input
✓ Data stays where it is
✓ 1-to-1 partition mapping

In Execution Plan:
✓ No "Exchange" operator
✓ Operations stacked vertically
✓ Can be pipelined together
"""

# ============================================================================
# PART 2: WIDE TRANSFORMATIONS (Require Shuffle)
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: WIDE TRANSFORMATIONS (Shuffle Required)")
print("=" * 80)

"""
WIDE Transformations:
- Data from MULTIPLE input partitions → output partition
- Requires SHUFFLE (expensive!)
- Data moves across network
- Writes to disk during shuffle

Examples:
- groupBy()      : Group by key (shuffle by key)
- join()         : Join DataFrames (shuffle both sides)
- distinct()     : Find unique values (shuffle)
- repartition()  : Change partition count (shuffle)
- sortWithinPartitions() : Sort globally (shuffle)
- aggregations   : Global aggregations need shuffle
"""

# Wide Transformation 1: groupBy
print("\n" + "-" * 80)
print("WIDE TRANSFORMATION 1: groupBy + agg")
print("-" * 80)
print("Grouping by category (data with same category goes to same partition)")

start = time.time()
df_grouped = df.groupBy("category").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
)
result = df_grouped.collect()
elapsed = time.time() - start

print(f"   Input partitions: {df.rdd.getNumPartitions()}")
print(f"   Output partitions: {df_grouped.rdd.getNumPartitions()}")
print(f"   Time: {elapsed:.4f}s")
print(f"   Result groups: {len(result)}")
print("   ⚠️  SHUFFLE occurred! (data moved between executors)")

print("\nExecution Plan (look for 'Exchange'):")
df_grouped.explain()

"""
What happened in groupBy:
1. Each partition computes partial aggregates locally
2. SHUFFLE: Data with same key goes to same partition
3. Final aggregates computed per partition
4. Exchange operator visible in plan

Shuffle Stages:
- Map side: Partition data by key, write to disk
- Shuffle: Transfer data over network
- Reduce side: Read from disk, compute final result
"""

# Wide Transformation 2: join
print("\n" + "-" * 80)
print("WIDE TRANSFORMATION 2: join")
print("-" * 80)

# Create second DataFrame
customer_data = [(f"CUST{i:04d}", f"Customer_{i}", ["Premium", "Regular", "Basic"][i % 3]) 
                 for i in range(1, 101)]
customers_df = spark.createDataFrame(customer_data, ["customer_id", "customer_name", "tier"])

print(f"Transactions partitions: {df.rdd.getNumPartitions()}")
print(f"Customers partitions: {customers_df.rdd.getNumPartitions()}")

start = time.time()
df_joined = df.join(customers_df, "customer_id", "inner")
result_count = df_joined.count()
elapsed = time.time() - start

print(f"\nAfter join:")
print(f"  Output partitions: {df_joined.rdd.getNumPartitions()}")
print(f"  Result records: {result_count}")
print(f"  Time: {elapsed:.4f}s")
print("  ⚠️  SHUFFLE on both sides!")

print("\nJoin Execution Plan:")
df_joined.explain()

"""
Join Types and Shuffle:
1. Sort-Merge Join:
   - Shuffle BOTH DataFrames by join key
   - Sort data on each side
   - Merge sorted data
   
2. Broadcast Join (optimization):
   - No shuffle if one side is small (<10MB)
   - Broadcast small table to all executors
   - Much faster for small lookups
"""

# Wide Transformation 3: distinct
print("\n" + "-" * 80)
print("WIDE TRANSFORMATION 3: distinct")
print("-" * 80)

start = time.time()
df_distinct = df.select("category").distinct()
distinct_count = df_distinct.count()
elapsed = time.time() - start

print(f"  Input partitions: {df.rdd.getNumPartitions()}")
print(f"  Output partitions: {df_distinct.rdd.getNumPartitions()}")
print(f"  Distinct categories: {distinct_count}")
print(f"  Time: {elapsed:.4f}s")
print("  ⚠️  SHUFFLE to find unique values")

"""
Why distinct() requires shuffle:
- Same values might be in different partitions
- Must bring all instances of each value together
- Check for duplicates across entire dataset
"""

# Wide Transformation 4: repartition
print("\n" + "-" * 80)
print("WIDE TRANSFORMATION 4: repartition")
print("-" * 80)

print(f"Current partitions: {df.rdd.getNumPartitions()}")

# Repartition to different number
df_repartitioned = df.repartition(20)
print(f"After repartition(20): {df_repartitioned.rdd.getNumPartitions()}")
print("  ⚠️  Full shuffle (redistributes all data)")

# Repartition by column (hash partitioning)
df_repartitioned_by_col = df.repartition(10, "customer_id")
print(f"After repartition(10, 'customer_id'): {df_repartitioned_by_col.rdd.getNumPartitions()}")
print("  ⚠️  Shuffle by hash(customer_id)")
print("  ✓ All records for same customer go to same partition")

"""
repartition() uses:
1. Increase parallelism: repartition(200) for large joins
2. Partition by key: repartition(50, "user_id") before groupBy("user_id")
3. Balance data: Fix skewed partitions

Note: Expensive! Only use when benefit > cost
"""

# ============================================================================
# PART 3: PARTITION INSPECTION
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: INSPECTING PARTITION DISTRIBUTION")
print("=" * 80)

"""
Technique: Use rdd.glom() to see data distribution
- glom() collects each partition into an array
- map(len) counts rows per partition
- collect() brings counts to driver
"""

def show_partition_distribution(df, name):
    """Show how data is distributed across partitions"""
    partition_sizes = df.rdd.glom().map(len).collect()
    print(f"\n{name}:")
    print(f"  Total partitions: {len(partition_sizes)}")
    print(f"  Total records: {sum(partition_sizes)}")
    print(f"  Partition sizes: {partition_sizes[:10]}")  # First 10
    print(f"  Min size: {min(partition_sizes)}")
    print(f"  Max size: {max(partition_sizes)}")
    print(f"  Avg size: {sum(partition_sizes)/len(partition_sizes):.1f}")
    
    # Check for skew
    max_size = max(partition_sizes)
    avg_size = sum(partition_sizes) / len(partition_sizes)
    if max_size > avg_size * 3:
        print(f"  ⚠️  DATA SKEW detected! (max is {max_size/avg_size:.1f}x average)")
    else:
        print(f"  ✓ Balanced distribution")

# Check original data
show_partition_distribution(df, "Original DataFrame")

# After groupBy (may have skew)
show_partition_distribution(df_grouped, "After groupBy")

# After repartition (should be balanced)
show_partition_distribution(df_repartitioned, "After repartition(20)")

"""
Learning Point - Partition Size:
- Ideal: 128MB - 1GB per partition
- Too small: Overhead dominates (task scheduling)
- Too large: Memory pressure, slow tasks
- Check with: df.rdd.glom().map(len).collect()
"""

# ============================================================================
# PART 4: CONTROLLING SHUFFLE PARTITIONS
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: SHUFFLE PARTITION TUNING")
print("=" * 80)

"""
spark.sql.shuffle.partitions:
- Controls number of partitions AFTER shuffle
- Default: 200 (often too high for small data)
- Should be tuned based on data size

Rule of thumb:
- Small data (<1GB): 50-100 partitions
- Medium data (1-10GB): 200-500 partitions  
- Large data (>10GB): 500-2000 partitions
"""

print("\nCurrent shuffle partitions setting:")
print(f"  spark.sql.shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

# Test with different shuffle partition counts
print("\nTesting groupBy with different shuffle partitions:")

for num_partitions in [5, 10, 50]:
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    
    start = time.time()
    result = df.groupBy("category").agg(count("*").alias("count"))
    output_parts = result.rdd.getNumPartitions()
    count = result.count()
    elapsed = time.time() - start
    
    print(f"  {num_partitions} shuffle partitions → {output_parts} output partitions, Time: {elapsed:.4f}s")

"""
Tuning Guidelines:

Too Few Partitions:
✗ Large tasks (OOM risk)
✗ Underutilized cluster
✗ Stragglers slow entire job

Too Many Partitions:
✗ Task scheduling overhead
✗ Many small files on disk
✗ Slower for small data

Sweet Spot:
✓ Partition size: 128MB - 1GB
✓ Number of partitions: 2-3x number of cores
✓ Adjust based on data size
"""

# Reset to default
spark.conf.set("spark.sql.shuffle.partitions", "10")

# ============================================================================
# PART 5: COALESCE vs REPARTITION
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: COALESCE vs REPARTITION")
print("=" * 80)

"""
coalesce(n):
- Reduce partitions WITHOUT full shuffle
- Combines partitions (doesn't redistribute)
- FAST but may create skew
- Use: Reduce partitions before writing

repartition(n):
- Full shuffle to redistribute data
- Creates balanced partitions
- SLOWER but more balanced
- Use: Increase partitions or fix skew
"""

# Create DataFrame with many partitions
df_many = df.repartition(50)
print(f"\nStarting with {df_many.rdd.getNumPartitions()} partitions")

# Method 1: coalesce (no full shuffle)
print("\n1. COALESCE (Reduce without full shuffle)")
start = time.time()
df_coalesced = df_many.coalesce(5)
elapsed_coalesce = time.time() - start

print(f"   After coalesce(5): {df_coalesced.rdd.getNumPartitions()} partitions")
print(f"   Time: {elapsed_coalesce:.4f}s")
show_partition_distribution(df_coalesced, "After coalesce")

# Method 2: repartition (full shuffle)
print("\n2. REPARTITION (Full shuffle)")
start = time.time()
df_repart = df_many.repartition(5)
elapsed_repart = time.time() - start

print(f"   After repartition(5): {df_repart.rdd.getNumPartitions()} partitions")
print(f"   Time: {elapsed_repart:.4f}s")
show_partition_distribution(df_repart, "After repartition")

print(f"\nSpeed comparison:")
print(f"  coalesce: {elapsed_coalesce:.4f}s (faster, may be skewed)")
print(f"  repartition: {elapsed_repart:.4f}s (slower, balanced)")

"""
When to use each:

COALESCE:
✓ Reducing partitions (50 → 5)
✓ Before writing files (fewer output files)
✓ Performance not critical
✗ Don't use to increase partitions (does nothing)

REPARTITION:
✓ Increasing partitions (5 → 50)
✓ Fixing data skew
✓ Need balanced distribution
✗ Expensive for large data
"""

# ============================================================================
# PART 6: PERFORMANCE COMPARISON
# ============================================================================

print("\n" + "=" * 80)
print("PART 6: PERFORMANCE COMPARISON")
print("=" * 80)

# Benchmark narrow vs wide operations
print("\nBenchmarking Narrow vs Wide transformations:")

# Narrow operations
print("\nNARROW Operations (Fast):")
start = time.time()
result_narrow = df.filter(col("amount") > 100) \
                  .select("customer_id", "amount", "category") \
                  .withColumn("amount_doubled", col("amount") * 2) \
                  .count()
time_narrow = time.time() - start
print(f"  Time: {time_narrow:.4f}s")

# Wide operations
print("\nWIDE Operations (Slower):")
start = time.time()
result_wide = df.groupBy("category") \
                .agg(sum("amount").alias("total")) \
                .orderBy(col("total").desc()) \
                .count()
time_wide = time.time() - start
print(f"  Time: {time_wide:.4f}s")

print(f"\nWide operations are {time_wide/time_narrow:.2f}x slower due to shuffle")

"""
Performance Impact:
- Narrow: ~10-100ms for millions of rows
- Wide: ~1-10s for millions of rows (10-100x slower)

Shuffle overhead comes from:
1. Writing intermediate data to disk
2. Transferring data over network
3. Reading shuffled data from disk
4. Task scheduling overhead
"""

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. NARROW TRANSFORMATIONS:
   ✓ No shuffle (fast)
   ✓ 1-to-1 partition mapping
   ✓ Examples: filter, select, map, withColumn
   ✓ Partition count unchanged
   ✓ Can pipeline multiple narrow ops

2. WIDE TRANSFORMATIONS:
   ⚠️  Require shuffle (slow)
   ⚠️  Data moves across network
   ⚠️  Examples: groupBy, join, distinct, repartition
   ⚠️  Creates new partitions
   ⚠️  Visible as "Exchange" in plan

3. SHUFFLE OPTIMIZATION:
   ✓ Tune spark.sql.shuffle.partitions
   ✓ Default 200 often too high for small data
   ✓ Target: 128MB-1GB per partition
   ✓ Rule: 2-3x number of cores

4. PARTITION MANAGEMENT:
   ✓ coalesce: Reduce partitions (no full shuffle)
   ✓ repartition: Change count (full shuffle)
   ✓ repartition(col): Partition by column
   ✓ Monitor with: rdd.glom().map(len).collect()

5. PERFORMANCE TIPS:
   ✓ Minimize shuffles (avoid unnecessary groupBy/join)
   ✓ Use broadcast joins for small tables
   ✓ Repartition by join key before joins
   ✓ Coalesce before writing to reduce files
   ✓ Check for data skew

6. ANTI-PATTERNS:
   ✗ Too many shuffle partitions for small data
   ✗ Unnecessary repartition() calls
   ✗ Not repartitioning skewed data
   ✗ Multiple groupBy on same key (cache first!)
   ✗ Joins without broadcast hint for small tables

7. MONITORING:
   ✓ Check Spark UI for shuffle reads/writes
   ✓ Look for "Exchange" in explain() output
   ✓ Monitor partition sizes
   ✓ Watch for data skew (uneven partitions)
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Remember: Shuffle is expensive! Minimize wide transformations.")
