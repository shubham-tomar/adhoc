"""
Problem 2.1: Lazy Evaluation Understanding
===========================================
Goal: Understand lazy evaluation and when Spark actually executes code

Key Concepts:
- Transformations are LAZY (create execution plan, don't execute)
- Actions are EAGER (trigger actual computation)
- Spark builds a DAG (Directed Acyclic Graph) of transformations
- Execution happens only when action is called
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 2.1 - Lazy Evaluation") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 2.1: LAZY EVALUATION AND TRANSFORMATIONS VS ACTIONS")
print("=" * 80)

# ============================================================================
# PART 1: Understanding Lazy Evaluation
# ============================================================================

print("\n" + "-" * 80)
print("PART 1: DEMONSTRATING LAZY EVALUATION")
print("-" * 80)

"""
LAZY EVALUATION: Spark delays execution until absolutely necessary

Why?
1. OPTIMIZATION: Build complete execution plan before running
2. EFFICIENCY: Combine multiple operations
3. SMART EXECUTION: Catalyst optimizer can rearrange operations

Key Principle: Transformations are lazy, Actions trigger execution
"""

# Read data - This is a transformation (LAZY)
print("\n1. Reading CSV (transformation - LAZY)...")
print("   → This creates a plan but doesn't read the file yet")
start = time.time()
df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
elapsed = time.time() - start
print(f"   Time taken: {elapsed:.4f}s (should be ~instant)")

"""
Learning Point: 
- Time is almost instant because NO actual reading happened
- Spark only registered "I need to read this file eventually"
- File is NOT loaded into memory yet
"""

# ============================================================================
# PART 2: Chain Multiple Transformations
# ============================================================================

print("\n" + "-" * 80)
print("PART 2: CHAINING TRANSFORMATIONS (All LAZY)")
print("-" * 80)

print("\nChaining transformations without triggering execution...")
start = time.time()

# Transformation 1: Filter
print("  1. filter(amount > 100) - LAZY")
df_filtered = df.filter(col("amount") > 100)

# Transformation 2: Select
print("  2. select(customer_id, amount, category) - LAZY")
df_selected = df_filtered.select("customer_id", "amount", "category")

# Transformation 3: Add derived column
print("  3. withColumn(amount_category) - LAZY")
df_with_tier = df_selected.withColumn(
    "amount_category",
    when(col("amount") > 500, "High")
    .when(col("amount") > 100, "Medium")
    .otherwise("Low")
)

# Transformation 4: Sort
print("  4. orderBy(amount desc) - LAZY")
df_sorted = df_with_tier.orderBy(col("amount").desc())

elapsed = time.time() - start
print(f"\nTime for all 4 transformations: {elapsed:.4f}s")
print("→ Still instant! No actual computation happened")

"""
Learning Point - Transformation Chaining:
- All transformations are just building an execution plan
- No data is processed yet
- No memory is allocated yet
- No I/O has occurred yet

Spark is saying: "I'll do all this... when you actually need the results"
"""

# ============================================================================
# PART 3: View the Execution Plan
# ============================================================================

print("\n" + "-" * 80)
print("PART 3: VIEWING THE EXECUTION PLAN")
print("-" * 80)

print("\nLogical Plan (what we want to do):")
df_sorted.explain(mode="simple")

print("\nPhysical Plan (how Spark will do it):")
df_sorted.explain(mode="formatted")

"""
The Plan Shows:
1. FileScan csv (read the file)
2. Filter (amount > 100)
3. Project (select columns + add amount_category)
4. Sort (order by amount desc)

But NONE of this has executed yet!
It's just a blueprint.
"""

# ============================================================================
# PART 4: Triggering Execution with Actions
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: ACTIONS TRIGGER EXECUTION")
print("=" * 80)

"""
ACTIONS: Operations that return results to driver or write to storage

Common Actions:
- count()      → Return number of rows
- show()       → Display first N rows
- collect()    → Return all data to driver (⚠️ dangerous with big data!)
- first()      → Return first row
- take(n)      → Return first n rows
- write.*      → Write to storage
"""

# ACTION 1: count()
print("\n" + "-" * 80)
print("ACTION 1: count()")
print("-" * 80)
print("NOW executing all transformations + counting rows...")
start = time.time()
count = df_sorted.count()
elapsed = time.time() - start
print(f"✅ Execution triggered! Found {count} rows")
print(f"   Time: {elapsed:.4f}s (actual work happened)")

"""
What happened:
1. Spark executed entire plan: read → filter → select → add column → sort
2. Counted the results
3. Returned single number to driver

Note: Sort was executed even though count() doesn't need sorting!
Spark executes the ENTIRE plan up to this point.
"""

# ACTION 2: show()
print("\n" + "-" * 80)
print("ACTION 2: show()")
print("-" * 80)
print("Displaying top 5 rows...")
start = time.time()
df_sorted.show(5)
elapsed = time.time() - start
print(f"Time: {elapsed:.4f}s")

"""
What happened:
1. Plan executed AGAIN (not cached)
2. Only fetched enough data to show 5 rows
3. Results displayed to console

Note: Each action re-executes unless you cache!
"""

# ACTION 3: first()
print("\n" + "-" * 80)
print("ACTION 3: first()")
print("-" * 80)
start = time.time()
first_row = df_sorted.first()
elapsed = time.time() - start
print(f"First row: {first_row}")
print(f"Time: {elapsed:.4f}s")

"""
first() is optimized:
- Executes plan but stops after finding first row
- More efficient than collect()[0]
"""

# ACTION 4: take(n)
print("\n" + "-" * 80)
print("ACTION 4: take(10)")
print("-" * 80)
start = time.time()
rows = df_sorted.take(10)
elapsed = time.time() - start
print(f"Retrieved {len(rows)} rows")
print(f"Time: {elapsed:.4f}s")

"""
take(n):
- Similar to show() but returns Row objects
- Can be used in code (show() just prints)
- Stops after finding n rows
"""

# ACTION 5: collect() ⚠️
print("\n" + "-" * 80)
print("ACTION 5: collect() ⚠️ DANGEROUS")
print("-" * 80)
print("Collecting ALL data to driver...")
start = time.time()
all_rows = df_sorted.collect()  # ⚠️ DON'T DO THIS WITH BIG DATA!
elapsed = time.time() - start
print(f"⚠️  Collected {len(all_rows)} rows to driver memory")
print(f"Time: {elapsed:.4f}s")

"""
collect() WARNING:
✗ Brings ALL data to driver JVM
✗ Can cause OutOfMemoryError
✗ Defeats distributed processing
✗ Only use with small result sets (<1GB)

Use instead:
✓ take(n) for sampling
✓ show(n) for viewing
✓ write.* for saving results
"""

# ============================================================================
# PART 5: Re-execution vs Caching
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: RE-EXECUTION vs CACHING")
print("=" * 80)

print("\n" + "-" * 80)
print("WITHOUT CACHING (re-executes every time)")
print("-" * 80)

df_test = spark.read.csv("transactions.csv", header=True, inferSchema=True) \
    .filter(col("amount") > 100)

print("\nFirst action (count):")
start = time.time()
count1 = df_test.count()
time1 = time.time() - start
print(f"  Count: {count1}, Time: {time1:.4f}s")

print("\nSecond action (count again):")
start = time.time()
count2 = df_test.count()
time2 = time.time() - start
print(f"  Count: {count2}, Time: {time2:.4f}s")

print(f"\n→ Plan executed TWICE! Each action re-reads and re-filters the data")

# Now with caching
print("\n" + "-" * 80)
print("WITH CACHING (compute once, reuse)")
print("-" * 80)

df_cached = spark.read.csv("transactions.csv", header=True, inferSchema=True) \
    .filter(col("amount") > 100) \
    .cache()  # ← Mark for caching

print("\nFirst action (count) - computes and caches:")
start = time.time()
count1 = df_cached.count()
time1 = time.time() - start
print(f"  Count: {count1}, Time: {time1:.4f}s (computed + cached)")

print("\nSecond action (count again) - uses cache:")
start = time.time()
count2 = df_cached.count()
time2 = time.time() - start
print(f"  Count: {count2}, Time: {time2:.4f}s (from cache)")

print(f"\n→ Second run is faster! Data is cached in memory")
print(f"→ Speedup: {time1/time2:.2f}x")

# Unpersist when done
df_cached.unpersist()

"""
Caching/Persistence:

cache() = persist(StorageLevel.MEMORY_AND_DISK)
- Stores DataFrame in memory
- Reuses cached data for subsequent actions
- Crucial for iterative algorithms

When to cache:
✓ DataFrame used multiple times
✓ Iterative algorithms (ML)
✓ Interactive analysis
✗ One-time use (waste of memory)
"""

# ============================================================================
# PART 6: Narrow vs Wide Transformations
# ============================================================================

print("\n" + "=" * 80)
print("PART 6: NARROW vs WIDE TRANSFORMATIONS")
print("=" * 80)

"""
NARROW Transformations:
- Each input partition → one output partition
- No data shuffle between partitions
- Examples: filter, map, select, withColumn

WIDE Transformations:
- Data from multiple input partitions → output partition
- Requires SHUFFLE (expensive!)
- Examples: groupBy, join, distinct, repartition
"""

print("\n" + "-" * 80)
print("NARROW Transformations (No Shuffle)")
print("-" * 80)

df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# Narrow operations
df_narrow = df \
    .filter(col("amount") > 100) \
    .select("customer_id", "amount") \
    .withColumn("amount_doubled", col("amount") * 2)

print(f"After narrow transformations: {df_narrow.rdd.getNumPartitions()}")
print("→ Same number of partitions (no shuffle)")

print("\nNarrow transformation plan:")
df_narrow.explain()

print("\n" + "-" * 80)
print("WIDE Transformations (Shuffle Required)")
print("-" * 80)

# Wide operation: groupBy
df_wide = df.groupBy("category").agg(
    count("*").alias("count"),
    avg("amount").alias("avg_amount")
)

print(f"After groupBy: {df_wide.rdd.getNumPartitions()} partitions")
print("→ Different number (shuffle happened)")

print("\nWide transformation plan (look for 'Exchange'):")
df_wide.explain()

"""
In the plan, look for:
- Exchange: Indicates shuffle
- HashAggregate: Aggregation operation

Shuffle is EXPENSIVE:
- Writes data to disk
- Transfers over network
- Sorts/groups data
- Takes 10-100x longer than narrow ops
"""

# ============================================================================
# PART 7: DAG Visualization
# ============================================================================

print("\n" + "=" * 80)
print("PART 7: EXECUTION DAG")
print("=" * 80)

complex_df = spark.read.csv("transactions.csv", header=True, inferSchema=True) \
    .filter(col("amount") > 100) \
    .select("customer_id", "amount", "category") \
    .groupBy("category") \
    .agg(sum("amount").alias("total")) \
    .orderBy(col("total").desc())

print("\nComplex transformation chain:")
print("  1. Read CSV")
print("  2. Filter (narrow)")
print("  3. Select (narrow)")
print("  4. GroupBy + Agg (wide - shuffle!)")
print("  5. OrderBy (wide - shuffle!)")

print("\nDAG stages:")
complex_df.explain()

"""
DAG (Directed Acyclic Graph):
- Spark breaks job into stages
- Stage boundary = shuffle (wide transformation)
- Each stage can run in parallel across partitions

Check Spark UI at localhost:4040 to see:
- DAG visualization
- Stage timeline
- Shuffle metrics
"""

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. LAZY EVALUATION:
   ✓ Transformations don't execute immediately
   ✓ Builds execution plan (DAG)
   ✓ Optimizes before executing
   ✓ Actions trigger actual computation

2. TRANSFORMATIONS (Lazy):
   ✓ filter, select, withColumn, map, flatMap
   ✓ groupBy, join, distinct, orderBy
   ✓ Return new DataFrame
   ✓ Chain multiple transformations

3. ACTIONS (Eager):
   ✓ count, show, first, take, collect
   ✓ write.*, save, saveAsTable
   ✓ Trigger execution
   ✓ Return results to driver

4. CACHING:
   ✓ Use cache() for DataFrames used multiple times
   ✓ First action computes + caches
   ✓ Subsequent actions use cache
   ✓ Don't forget unpersist() when done

5. NARROW vs WIDE:
   ✓ Narrow: No shuffle (fast)
   ✓ Wide: Requires shuffle (slow)
   ✓ Minimize shuffles for performance

6. BEST PRACTICES:
   ✓ Chain transformations before actions
   ✓ Use explain() to understand execution
   ✓ Cache DataFrames used multiple times
   ✓ Avoid collect() on large datasets
   ✓ Use take/show for sampling
   ✓ Monitor Spark UI for shuffle metrics

7. COMMON MISTAKES:
   ✗ collect() on large data (OOM)
   ✗ Not caching reused DataFrames
   ✗ Too many small shuffles
   ✗ Not checking execution plans
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Pro Tip: Always check Spark UI at http://localhost:4040")
print("   to visualize DAG, stages, and shuffle metrics!")
