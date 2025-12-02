"""
Problem 1.3: Advanced Reading Options
======================================
Goal: Use column pruning and predicate pushdown for optimized reading

Key Concepts:
- Column pruning: Read only needed columns (saves I/O and memory)
- Predicate pushdown: Filter at source (reduces data transferred)
- Query optimization verification using execution plans
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Problem 1.3 - Advanced Reading") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# Set log level to see optimization details
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("PROBLEM 1.3: COLUMN PRUNING AND PREDICATE PUSHDOWN")
print("=" * 80)

# ============================================================================
# PART 1: BASELINE - Read Everything
# ============================================================================

print("\n" + "-" * 80)
print("BASELINE: Reading All Columns and Rows")
print("-" * 80)

"""
Learning Point: Full Table Scan
- Reads ALL columns from storage
- Reads ALL rows (then filters in memory)
- INEFFICIENT for large datasets
"""

print("\nReading ALL data from Parquet...")
df_all = spark.read.parquet("transactions.parquet")

print(f"Schema has {len(df_all.columns)} columns:")
print(df_all.columns)

print(f"\nTotal records: {df_all.count()}")

# Apply filter in Spark (NOT at source)
df_filtered_spark = df_all.filter(col("amount") > 500)
print(f"Records with amount > 500: {df_filtered_spark.count()}")

print("\nExecution Plan (Full Scan):")
df_filtered_spark.explain(True)

"""
In the plan, you'll see:
- FileScan parquet: Reads ALL columns
- Filter (amount > 500): Applied AFTER reading data
- This is INEFFICIENT
"""

# ============================================================================
# PART 2: COLUMN PRUNING
# ============================================================================

print("\n" + "=" * 80)
print("OPTIMIZATION 1: COLUMN PRUNING")
print("=" * 80)

"""
Column Pruning: Read only columns you need

Benefits:
✓ Less data read from disk (I/O reduction)
✓ Less data in memory (memory reduction)
✓ Faster processing
✓ Automatic with Parquet (columnar format)

How it works:
- Parquet stores data column-by-column
- Spark reads only requested columns
- Other columns are skipped entirely
"""

print("\nReading ONLY 3 columns (transaction_id, amount, category)...")

# Method 1: Select during read (explicit)
df_pruned = spark.read.parquet("transactions.parquet") \
    .select("transaction_id", "amount", "category")

print("\nColumns read:")
print(df_pruned.columns)

df_pruned.show(5)

# Check execution plan
print("\nExecution Plan with Column Pruning:")
df_pruned.explain()

"""
In the plan, look for:
- FileScan parquet [...selected columns...]
- Only shows: transaction_id, amount, category
- ReadSchema: Lists only these 3 columns

This proves column pruning is working!
"""

# ============================================================================
# PART 3: PREDICATE PUSHDOWN
# ============================================================================

print("\n" + "=" * 80)
print("OPTIMIZATION 2: PREDICATE PUSHDOWN")
print("=" * 80)

"""
Predicate Pushdown: Push filters to data source

Benefits:
✓ Filter at source (before reading into memory)
✓ Reads only matching rows
✓ Reduces network transfer
✓ Reduces memory usage

Supported by:
✓ Parquet (using column statistics)
✓ ORC
✓ JDBC sources
✗ CSV (must read everything)
✗ JSON (must read everything)
"""

print("\nApplying filter DURING read (predicate pushdown)...")

# Combine column pruning + predicate pushdown
df_optimized = spark.read.parquet("transactions.parquet") \
    .select("transaction_id", "amount", "category") \
    .filter(col("amount") > 500)

print(f"Records after filter: {df_optimized.count()}")
df_optimized.show(5)

# ============================================================================
# PART 4: VERIFY OPTIMIZATIONS IN PHYSICAL PLAN
# ============================================================================

print("\n" + "=" * 80)
print("VERIFICATION: Physical Plan Analysis")
print("=" * 80)

print("\nPhysical Plan (showing optimizations):")
df_optimized.explain(mode="formatted")

"""
What to look for in the plan:

1. COLUMN PRUNING:
   ReadSchema: struct<transaction_id:string,amount:double,category:string>
   ↑ Only 3 columns listed (not all 9)

2. PREDICATE PUSHDOWN:
   PushedFilters: [IsNotNull(amount), GreaterThan(amount,500.0)]
   ↑ Filter is "pushed" to file scan level

3. DATA SKIPPING:
   Parquet uses column statistics to skip entire row groups
   where amount <= 500
"""

# More detailed plan
print("\n" + "-" * 80)
print("Extended Explanation:")
print("-" * 80)
df_optimized.explain(mode="extended")

# ============================================================================
# PART 5: PERFORMANCE COMPARISON
# ============================================================================

print("\n" + "=" * 80)
print("PERFORMANCE COMPARISON")
print("=" * 80)

import time

def benchmark_read(description, read_func):
    """Benchmark a read operation"""
    print(f"\n{description}")
    start = time.time()
    count = read_func().count()  # Force execution
    elapsed = time.time() - start
    print(f"  Time: {elapsed:.4f}s | Records: {count}")
    return elapsed

# Test 1: Full scan (all columns, no filter)
time1 = benchmark_read(
    "1. Full Scan (all columns):",
    lambda: spark.read.parquet("transactions.parquet")
)

# Test 2: Column pruning only
time2 = benchmark_read(
    "2. Column Pruning (3 columns):",
    lambda: spark.read.parquet("transactions.parquet").select("transaction_id", "amount", "category")
)

# Test 3: Predicate pushdown only
time3 = benchmark_read(
    "3. Predicate Pushdown (filter only):",
    lambda: spark.read.parquet("transactions.parquet").filter(col("amount") > 500)
)

# Test 4: Both optimizations
time4 = benchmark_read(
    "4. Both Optimizations (pruning + pushdown):",
    lambda: spark.read.parquet("transactions.parquet")
              .select("transaction_id", "amount", "category")
              .filter(col("amount") > 500)
)

# Print comparison
print("\n" + "-" * 80)
print("Speed Comparison:")
print("-" * 80)
print(f"Full Scan:              {time1:.4f}s  (baseline)")
print(f"Column Pruning:         {time2:.4f}s  ({time2/time1:.2f}x)")
print(f"Predicate Pushdown:     {time3:.4f}s  ({time3/time1:.2f}x)")
print(f"Both Optimizations:     {time4:.4f}s  ({time4/time1:.2f}x)  ← FASTEST")

"""
Expected Results:
- Both optimizations together = FASTEST
- Typically 2-10x faster than full scan
- Actual speedup depends on:
  - Selectivity of filter (% of rows matching)
  - Number of columns (% of columns selected)
  - File format and compression
"""

# ============================================================================
# PART 6: PARQUET STATISTICS
# ============================================================================

print("\n" + "=" * 80)
print("HOW PREDICATE PUSHDOWN WORKS: Parquet Statistics")
print("=" * 80)

"""
Parquet files store min/max statistics for each column in each row group.

Example row group statistics:
┌──────────────┬─────────┬─────────┬──────────┐
│ Row Group    │ Column  │ Min     │ Max      │
├──────────────┼─────────┼─────────┼──────────┤
│ Group 0      │ amount  │ 10.50   │ 450.75   │
│ Group 1      │ amount  │ 475.00  │ 899.99   │  ← Some matches
│ Group 2      │ amount  │ 520.00  │ 985.00   │  ← All match!
│ Group 3      │ amount  │ 15.25   │ 495.00   │
└──────────────┴─────────┴─────────┴──────────┘

Query: WHERE amount > 500

Spark logic:
- Group 0: max=450.75 < 500 → SKIP entire group
- Group 1: Check rows (some may match)
- Group 2: min=520 > 500 → READ all rows (all match)
- Group 3: max=495 < 500 → SKIP entire group

This is called "DATA SKIPPING" or "ROW GROUP PRUNING"
"""

print("""
Parquet Predicate Pushdown:
1. Spark reads Parquet file metadata (min/max per row group)
2. Evaluates filter against statistics
3. Skips row groups that can't contain matching data
4. Only reads row groups that might have matches
5. Applies filter to actual data in memory

Result: Reads only ~30-70% of data (depending on selectivity)
""")

# ============================================================================
# PART 7: PRACTICAL EXAMPLES
# ============================================================================

print("\n" + "=" * 80)
print("PRACTICAL EXAMPLES")
print("=" * 80)

# Example 1: Date range query with pushdown
print("\n1. Date Range Query with Pushdown:")
df_date_range = spark.read.parquet("transactions.parquet") \
    .select("transaction_id", "transaction_date", "amount") \
    .filter((col("transaction_date") >= "2024-01-01") & (col("transaction_date") < "2024-02-01"))

print(f"January 2024 transactions: {df_date_range.count()}")
print("Plan shows pushdown:")
df_date_range.explain()

# Example 2: Multiple filters with AND
print("\n2. Multiple Filters (AND condition):")
df_multi_filter = spark.read.parquet("transactions.parquet") \
    .select("transaction_id", "amount", "category", "region") \
    .filter(
        (col("amount") > 500) &
        (col("category") == "Electronics") &
        (col("region").isin(["North", "South"]))
    )

print(f"Filtered records: {df_multi_filter.count()}")
print("Multiple predicates pushed down:")
df_multi_filter.explain()

# Example 3: Column projection with complex expressions
print("\n3. Projection with Derived Columns:")
df_projection = spark.read.parquet("transactions.parquet") \
    .select(
        "transaction_id",
        col("amount"),
        (col("amount") * col("quantity")).alias("total_value"),
        when(col("amount") > 500, "High").otherwise("Normal").alias("tier")
    ) \
    .filter(col("amount") > 100)

df_projection.show(5)
print("Plan (derived columns computed AFTER pushdown):")
df_projection.explain()

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. COLUMN PRUNING:
   ✓ Read only columns you need with .select()
   ✓ Works automatically with Parquet/ORC
   ✓ Reduces I/O, memory, and processing time
   ✓ No code changes needed - Spark optimizes automatically

2. PREDICATE PUSHDOWN:
   ✓ Filter data at source before reading
   ✓ Use .filter() or WHERE clause
   ✓ Parquet uses statistics for data skipping
   ✓ Can reduce data read by 50-90%

3. BEST PRACTICES:
   ✓ Always select only needed columns
   ✓ Apply filters as early as possible
   ✓ Use Parquet for analytical workloads
   ✓ Verify optimizations with .explain()

4. VERIFICATION:
   ✓ Check physical plan for "PushedFilters"
   ✓ Check "ReadSchema" for column pruning
   ✓ Use .explain(mode="formatted") for details

5. WHEN OPTIMIZATIONS WORK:
   ✓ Parquet, ORC: Both optimizations
   ✓ JDBC: Predicate pushdown to database
   ✗ CSV, JSON: No predicate pushdown (must read all)

6. FILE FORMAT CHOICE:
   ✓ Use Parquet for Spark analytics (best optimization support)
   ✓ Compress with snappy (good balance of speed/size)
   ✓ Partition large tables by date/region
"""

print(takeaways)

# ============================================================================
# BONUS: Explain Modes
# ============================================================================

print("\n" + "=" * 80)
print("BONUS: Understanding .explain() Modes")
print("=" * 80)

df = spark.read.parquet("transactions.parquet") \
    .select("amount", "category") \
    .filter(col("amount") > 500)

print("\n1. Simple (default):")
df.explain()

print("\n2. Extended (all plans):")
df.explain(mode="extended")

print("\n3. Formatted (tree structure):")
df.explain(mode="formatted")

print("\n4. Cost (with statistics):")
df.explain(mode="cost")

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
