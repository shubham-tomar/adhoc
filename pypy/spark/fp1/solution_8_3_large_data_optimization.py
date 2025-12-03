"""
Problem 8.3: Memory and Disk Optimization
==========================================
Goal: Process data larger than available memory

Key Concepts:
- External sort
- Disk-based operations
- Partition pruning
- Projection pushdown
- Memory-efficient processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Problem 8.3 - Large Data Optimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 8.3: PROCESSING LARGE DATA (>MEMORY)")
print("=" * 80)

# ============================================================================
# TASK 1: Efficient Large File Reading
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: EFFICIENT READING STRATEGIES")
print("=" * 80)

"""
Techniques for large datasets:
1. Column pruning (read only needed columns)
2. Predicate pushdown (filter at source)
3. Partition filtering
4. Incremental processing
"""

# Simulate large dataset
df_large = spark.read.parquet("transactions.parquet")

# Bad: Read everything
print("\n❌ BAD: Reading all columns")
print("   df = spark.read.parquet('large_data')")
print("   df.filter(...).select(...)")

# Good: Read only what you need
print("\n✅ GOOD: Column pruning + predicate pushdown")

# Only read needed columns with filter pushed down
df_optimized = spark.read.parquet("transactions.parquet") \
    .select("transaction_id", "customer_id", "amount", "transaction_date") \
    .filter(col("amount") > 100)

print("   df = spark.read.parquet('large_data')")
print("       .select('id', 'amount', 'date')")
print("       .filter(col('amount') > 100)")

print("\nPhysical Plan (verify pushdown):")
df_optimized.explain()

"""
Benefits:
✓ Less data read from disk
✓ Less memory usage
✓ Faster processing
✓ Parquet columnar format enables this
"""

# ============================================================================
# TASK 2: Partition-based Processing
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: PARTITION-BASED PROCESSING")
print("=" * 80)

"""
Process data partition by partition to avoid loading all into memory
"""

# Write partitioned data
output_partitioned = "output/large_data_partitioned"

df_large.withColumn("year", year(to_date("transaction_date"))) \
        .withColumn("month", month(to_date("transaction_date"))) \
        .write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_partitioned)

print(f"✓ Data partitioned by year/month at: {output_partitioned}")

# Process one partition at a time
print("\nProcessing one partition at a time:")

years = [2024]
months = list(range(1, 13))

for year in years:
    for month in months:
        # Only load one partition
        df_partition = spark.read.parquet(output_partitioned) \
            .filter((col("year") == year) & (col("month") == month))
        
        count = df_partition.count()
        if count > 0:
            print(f"  {year}-{month:02d}: {count} records")
            
            # Process this partition
            # result = df_partition.transform(your_logic)
            # result.write.mode("append").parquet("output")

print("\n✓ Memory usage stays constant (only one partition in memory)")

# ============================================================================
# TASK 3: Streaming-style Processing
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: STREAMING-STYLE BATCH PROCESSING")
print("=" * 80)

"""
Process large batch data in streaming fashion
- Process in chunks
- Never materialize full dataset
- Constant memory usage
"""

print("\nChunked processing pattern:")

chunk_size = 1000

# Process in chunks using DataFrame operations
window_chunk = Window.orderBy("transaction_id").rowsBetween(0, chunk_size - 1)

df_chunked = df_large.withColumn(
    "chunk_id",
    (row_number().over(Window.orderBy("transaction_id")) / chunk_size).cast("int")
)

print(f"  Total chunks: {df_chunked.select('chunk_id').distinct().count()}")
print("  Processing each chunk separately...")

# Process each chunk
for chunk_id in range(3):  # Process first 3 chunks as example
    chunk_df = df_chunked.filter(col("chunk_id") == chunk_id)
    count = chunk_df.count()
    print(f"  Chunk {chunk_id}: {count} records")

# ============================================================================
# TASK 4: External Sort (Disk-based)
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: EXTERNAL SORT")
print("=" * 80)

"""
Sorting data larger than memory:
- Spark automatically spills to disk
- Monitor spill metrics
- Optimize with more partitions
"""

print("\nExternal sort configuration:")
print(f"  shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Large sort operation
print("\nSorting large dataset...")

df_sorted = df_large.orderBy(
    col("customer_id"),
    col("transaction_date").desc(),
    col("amount").desc()
)

# Write sorted data
sorted_path = "output/sorted_data"
df_sorted.write.mode("overwrite").parquet(sorted_path)

print(f"✓ Sorted data written to: {sorted_path}")
print("✓ Spark automatically used external sort (disk)")

"""
Spark's External Sort:
1. Partition data by sort key
2. Sort within each partition (in memory)
3. Spill to disk if partition too large
4. Merge sorted partitions

Monitor:
- Spark UI → Stages → Spill (Memory)
- Spark UI → Stages → Spill (Disk)
"""

# ============================================================================
# TASK 5: Memory-Efficient Aggregations
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: MEMORY-EFFICIENT AGGREGATIONS")
print("=" * 80)

"""
Techniques:
1. Partial aggregation (map-side combine)
2. Approximate aggregations
3. Incremental aggregation
"""

# Exact aggregation (more memory)
print("\n1. Exact Aggregation:")
exact_agg = df_large.groupBy("customer_id").agg(
    count("*").alias("count"),
    sum("amount").alias("total"),
    avg("amount").alias("avg"),
    collect_list("category").alias("categories")  # Memory-intensive!
)

print("   ⚠️  collect_list can cause OOM on large groups")

# Approximate aggregation (less memory)
print("\n2. Approximate Aggregation:")
approx_agg = df_large.groupBy("customer_id").agg(
    count("*").alias("count"),
    sum("amount").alias("total"),
    avg("amount").alias("avg"),
    approx_count_distinct("product_id").alias("distinct_products"),  # Approximate
    expr("percentile_approx(amount, 0.5)").alias("median_amount")  # Approximate
)

print("   ✓ Uses less memory")
print("   ✓ ~99% accuracy")
print("   ✓ Much faster")

approx_agg.show(5)

# ============================================================================
# TASK 6: Partition Pruning and Projection
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: OPTIMIZATION VERIFICATION")
print("=" * 80)

"""
Verify optimizations in execution plan
"""

# Read partitioned data with filters
df_pruned = spark.read.parquet(output_partitioned) \
    .select("customer_id", "amount") \
    .filter((col("year") == 2024) & (col("month") == 1))

print("\nQuery with partition pruning + column pruning:")
print("Plan should show:")
print("  ✓ PartitionFilters (year, month)")
print("  ✓ ReadSchema (only customer_id, amount)")

df_pruned.explain()

# ============================================================================
# TASK 7: Broadcast to Avoid Shuffle
# ============================================================================

print("\n" + "=" * 80)
print("TASK 7: BROADCAST OPTIMIZATION")
print("=" * 80)

"""
Small lookup tables: Broadcast to avoid shuffle
Large fact table: Never brought into memory fully
"""

# Small dimension table
lookup_data = [
    ("Premium", 0.9),
    ("Regular", 0.95),
    ("Basic", 1.0)
]
df_discount = spark.createDataFrame(lookup_data, ["tier", "discount_factor"])

print(f"\nLookup table size: {df_discount.count()} rows")
print("  ✓ Small enough to broadcast")

# Broadcast join
df_with_discount = df_large.join(
    broadcast(df_discount),
    df_large.category == df_discount.tier,
    "left"
).withColumn(
    "discounted_amount",
    col("amount") * coalesce(col("discount_factor"), lit(1.0))
)

print("\nBroadcast join (no shuffle of large table):")
df_with_discount.explain()

# ============================================================================
# TASK 8: Monitor and Tune
# ============================================================================

print("\n" + "=" * 80)
print("TASK 8: MONITORING LARGE DATA PROCESSING")
print("=" * 80)

monitoring_guide = """
SPARK UI METRICS TO WATCH:

1. MEMORY:
   □ Executor memory usage
   □ Storage memory (cached data)
   □ Execution memory (shuffles, sorts)
   □ Spilled records (disk overflow)

2. DISK:
   □ Shuffle write size
   □ Shuffle read size
   □ Spill to disk
   □ Data locality

3. TASKS:
   □ Task duration distribution
   □ Stragglers (slow tasks)
   □ Failed tasks
   □ GC time (< 10% ideal)

4. STAGES:
   □ Number of stages
   □ Shuffle operations
   □ Skipped stages (cached)
   □ Exchange operators

OPTIMIZATION CHECKLIST:

□ Read only needed columns (projection)
□ Filter early (predicate pushdown)
□ Use partitioned tables
□ Broadcast small tables
□ Use approximate functions where possible
□ Monitor spill metrics
□ Increase partitions if OOM
□ Cache intelligently
□ Clear cache when done
□ Use external sort for large sorts
"""

print(monitoring_guide)

# ============================================================================
# TASK 9: Best Practices Summary
# ============================================================================

print("\n" + "=" * 80)
print("TASK 9: BEST PRACTICES FOR LARGE DATA")
print("=" * 80)

best_practices = """
1. READ OPTIMIZATION:
   ✓ Column pruning (select only needed)
   ✓ Predicate pushdown (filter at source)
   ✓ Partition filtering
   ✓ Use Parquet/ORC (columnar)

2. MEMORY MANAGEMENT:
   ✓ Increase shuffle partitions
   ✓ Increase executor memory
   ✓ Use disk-based operations
   ✓ Avoid collect() on large data
   ✓ Use take() for sampling

3. PROCESSING PATTERNS:
   ✓ Partition-based processing
   ✓ Streaming-style batches
   ✓ Incremental aggregation
   ✓ Approximate functions

4. JOIN OPTIMIZATION:
   ✓ Broadcast small tables
   ✓ Pre-partition by join key
   ✓ Handle skew with salting

5. AGGREGATION:
   ✓ Partial aggregation (map-side)
   ✓ Approximate functions
   ✓ Two-phase aggregation
   ✓ Avoid collect_list on large groups

6. SORTING:
   ✓ Let Spark use external sort
   ✓ Increase shuffle partitions
   ✓ Monitor spill metrics
   ✓ Use sortWithinPartitions when possible

7. OUTPUT:
   ✓ Partition large tables
   ✓ Control file sizes
   ✓ Use compression
   ✓ Coalesce before writing

8. MONITORING:
   ✓ Watch Spark UI metrics
   ✓ Monitor spill to disk
   ✓ Check GC time
   ✓ Identify stragglers
"""

print(best_practices)

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. READING LARGE DATA:
   ✓ Projection pushdown (columns)
   ✓ Predicate pushdown (filters)
   ✓ Partition pruning
   ✓ Never read what you don't need

2. MEMORY CONSTRAINTS:
   ✓ Process partitions separately
   ✓ Use streaming patterns
   ✓ Increase partitions
   ✓ Monitor spill metrics

3. EXTERNAL OPERATIONS:
   ✓ External sort (automatic)
   ✓ Disk-based shuffles
   ✓ Spill to disk when needed
   ✓ Spark handles this automatically

4. EFFICIENT AGGREGATIONS:
   ✓ Partial aggregation
   ✓ Approximate functions (99% accurate)
   ✓ Avoid memory-intensive ops
   ✓ Two-phase when needed

5. OPTIMIZATION:
   ✓ Broadcast small tables
   ✓ Pre-partition data
   ✓ Cache strategically
   ✓ Use compression

6. MONITORING:
   ✓ Spark UI is essential
   ✓ Watch spill metrics
   ✓ Monitor GC time
   ✓ Identify bottlenecks

7. ANTI-PATTERNS:
   ✗ collect() on large data
   ✗ Too few partitions
   ✗ Not using column pruning
   ✗ Ignoring spill warnings
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Spark can handle data >> memory if configured correctly!")
print("🎓 Congratulations! You've completed all 19 exercises!")
print("\n📚 You now have production-ready PySpark skills!")
