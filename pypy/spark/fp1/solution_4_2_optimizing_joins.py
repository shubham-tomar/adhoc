"""
Problem 4.2: Optimizing for Joins
==================================
Goal: Prepare data optimally for join operations

Key Concepts:
- Broadcast joins for small tables
- Sort-merge joins
- Pre-partitioning by join key
- Join strategies and optimization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 4.2 - Optimizing Joins") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 4.2: OPTIMIZING FOR JOINS")
print("=" * 80)

# ============================================================================
# SETUP: Create DataFrames
# ============================================================================

print("\nSetting up datasets...")

# Large DataFrame: Transactions
df_transactions = spark.read.parquet("transactions.parquet")
print(f"Transactions: {df_transactions.count()} records")

# Small DataFrame: Customers (for broadcast join)
import random
customer_data = [(f"CUST{i:04d}", f"Customer_{i}", 
                  random.choice(["Premium", "Regular", "Basic"]),
                  random.choice(["USA", "Canada", "UK", "Germany"]))
                 for i in range(1, 1001)]
df_customers = spark.createDataFrame(
    customer_data, 
    ["customer_id", "customer_name", "tier", "country"]
)
print(f"Customers: {df_customers.count()} records")

# Medium DataFrame: Products
product_data = [(f"PROD{i:03d}", f"Product_{i}",
                 random.choice(["Electronics", "Clothing", "Books", "Home", "Sports"]),
                 round(random.uniform(10, 1000), 2))
                for i in range(1, 101)]
df_products = spark.createDataFrame(
    product_data,
    ["product_id", "product_name", "category", "price"]
)
print(f"Products: {df_products.count()} records")

# ============================================================================
# PART 1: Default Join (Without Optimization)
# ============================================================================

print("\n" + "=" * 80)
print("PART 1: BASELINE - DEFAULT JOIN")
print("=" * 80)

"""
Default Join Behavior:
- Spark chooses strategy based on table sizes
- May use sort-merge join (shuffle both sides)
- Can be slow for large tables
"""

print("\nPerforming default join (transactions + customers)...")

start = time.time()
df_joined_default = df_transactions.join(df_customers, "customer_id", "inner")
result_count = df_joined_default.count()
time_default = time.time() - start

print(f"  Records: {result_count}")
print(f"  Time: {time_default:.4f}s")

print("\nExecution Plan (look for join strategy):")
df_joined_default.explain()

"""
Reading the Plan:

SortMergeJoin:
- Both sides shuffled by join key
- Data sorted on each side
- Merge sorted data
- Good for large-large joins

BroadcastHashJoin:
- Small table broadcast to all nodes
- No shuffle needed
- Much faster
- Only for small tables (<10MB default)
"""

# ============================================================================
# PART 2: Broadcast Join (Small Table Optimization)
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: BROADCAST JOIN")
print("=" * 80)

"""
Broadcast Join:
- Send small table to all executor nodes
- No shuffle of large table
- Much faster for small dimension tables
- Limited by driver and executor memory
"""

print("\nForcing broadcast join with hint...")

start = time.time()
df_broadcast = df_transactions.join(
    broadcast(df_customers),  # ← Force broadcast
    "customer_id",
    "inner"
)
result_count = df_broadcast.count()
time_broadcast = time.time() - start

print(f"  Records: {result_count}")
print(f"  Time: {time_broadcast:.4f}s")
print(f"  Speedup: {time_default/time_broadcast:.2f}x faster")

print("\nExecution Plan (should show BroadcastHashJoin):")
df_broadcast.explain()

"""
Broadcast Join Advantages:
✓ No shuffle of large table
✓ 2-10x faster than sort-merge
✓ Less disk I/O
✓ Less network transfer

When to Use:
✓ One table < 10MB (default threshold)
✓ Dimension table lookups
✓ Fact-dimension joins
✓ Small reference data

Limitations:
✗ Table must fit in driver + executor memory
✗ Not suitable for large tables
✗ OOM risk if table too large

Configuration:
spark.sql.autoBroadcastJoinThreshold = 10MB (default)
Set to -1 to disable auto-broadcast
"""

# Check current threshold
threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"\nBroadcast threshold: {int(threshold)/(1024*1024):.1f} MB")

# ============================================================================
# PART 3: Pre-Partitioning by Join Key
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: PRE-PARTITIONING OPTIMIZATION")
print("=" * 80)

"""
Strategy: Partition both DataFrames by join key BEFORE join
- Co-locate records with same key
- Reduces shuffle during join
- Useful when joining same key multiple times
"""

print("\nPre-partitioning both DataFrames by customer_id...")

# Partition transactions
print("  1. Partitioning transactions by customer_id...")
df_trans_partitioned = df_transactions.repartition(20, "customer_id")
df_trans_partitioned.cache()  # Cache to avoid re-computing
df_trans_partitioned.count()  # Materialize cache

# Partition customers
print("  2. Partitioning customers by customer_id...")
df_cust_partitioned = df_customers.repartition(20, "customer_id")
df_cust_partitioned.cache()
df_cust_partitioned.count()

# Now join (should be faster)
print("  3. Performing join on pre-partitioned data...")
start = time.time()
df_optimized = df_trans_partitioned.join(df_cust_partitioned, "customer_id", "inner")
result_count = df_optimized.count()
time_optimized = time.time() - start

print(f"\n  Records: {result_count}")
print(f"  Time: {time_optimized:.4f}s")

print("\nExecution Plan (less shuffle needed):")
df_optimized.explain()

"""
Pre-Partitioning Benefits:
✓ Co-located data (same key in same partition)
✓ Less data movement during join
✓ Reusable for multiple joins on same key

When to Use:
✓ Multiple joins on same key
✓ Large-large table joins
✓ Iterative operations (ML pipelines)

Trade-off:
⚠️  Initial partitioning cost
⚠️  Need to cache partitioned data
✓ Pays off when reusing partitioned data
"""

# Cleanup cache
df_trans_partitioned.unpersist()
df_cust_partitioned.unpersist()

# ============================================================================
# PART 4: Join Type Comparison
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: JOIN TYPES AND STRATEGIES")
print("=" * 80)

"""
Join Types:
- inner: Only matching records
- left/left_outer: All from left + matches from right
- right/right_outer: All from right + matches from left
- full/full_outer: All records from both
- left_semi: Like inner but only left columns
- left_anti: Records in left NOT in right
- cross: Cartesian product (dangerous!)
"""

print("\nComparing join strategies...")

# Inner Join
df_inner = df_transactions.join(df_customers, "customer_id", "inner")
print(f"\n1. INNER JOIN: {df_inner.count()} records")

# Left Join
df_left = df_transactions.join(df_customers, "customer_id", "left")
print(f"2. LEFT JOIN: {df_left.count()} records")

# Left Semi (more efficient than inner for existence check)
df_semi = df_transactions.join(df_customers, "customer_id", "left_semi")
print(f"3. LEFT SEMI: {df_semi.count()} records")

# Left Anti (find unmatched)
df_anti = df_transactions.join(df_customers, "customer_id", "left_anti")
print(f"4. LEFT ANTI: {df_anti.count()} records (transactions without customer)")

"""
Join Strategy Selection:

left_semi vs inner:
- left_semi: Only need existence check
- inner: Need columns from both sides
- left_semi is more efficient if only using left columns

Example:
# Want transactions from known customers only
df.join(customers, "id", "left_semi")  # ✓ Efficient
df.join(customers, "id", "inner").select(df["*"])  # ✗ Wasteful

left_anti: Find missing data
- Transactions without customers
- Data quality checks
- Orphaned records
"""

# ============================================================================
# PART 5: Multi-Table Joins
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: MULTI-TABLE JOINS")
print("=" * 80)

"""
Joining 3+ tables:
- Order matters for performance
- Start with largest table
- Broadcast small dimensions
- Consider pre-partitioning
"""

print("\nJoining transactions + customers + products...")

# Approach 1: Sequential joins (not optimized)
print("\n1. Sequential Joins:")
start = time.time()
df_multi_seq = df_transactions \
    .join(df_customers, "customer_id", "left") \
    .join(df_products, "product_id", "left")
result = df_multi_seq.count()
time_seq = time.time() - start
print(f"   Time: {time_seq:.4f}s")

# Approach 2: Broadcast small tables
print("\n2. With Broadcast Hints:")
start = time.time()
df_multi_opt = df_transactions \
    .join(broadcast(df_customers), "customer_id", "left") \
    .join(broadcast(df_products), "product_id", "left")
result = df_multi_opt.count()
time_opt = time.time() - start
print(f"   Time: {time_opt:.4f}s")
print(f"   Speedup: {time_seq/time_opt:.2f}x")

"""
Multi-Join Best Practices:

1. Join Order:
   ✓ Start with largest table
   ✓ Broadcast small dimensions
   ✓ Filter early

2. Example:
   large_fact (10M rows)
   .join(broadcast(dim1), "key1")  # 1K rows
   .join(broadcast(dim2), "key2")  # 500 rows
   .filter(conditions)

3. Avoid:
   ✗ Cross joins (unless very small)
   ✗ Joining on non-indexed columns
   ✗ Not broadcasting small tables
"""

# ============================================================================
# PART 6: Join Condition Complexity
# ============================================================================

print("\n" + "=" * 80)
print("PART 6: COMPLEX JOIN CONDITIONS")
print("=" * 80)

"""
Join conditions can be:
- Simple equality: df1.join(df2, "key")
- Multiple keys: df1.join(df2, ["key1", "key2"])
- Complex conditions: df1.join(df2, condition_expr)
"""

print("\n1. Simple equality join:")
simple = df_transactions.join(df_customers, "customer_id")
print(f"   Records: {simple.count()}")

print("\n2. Multiple column join:")
# Add region to both for demo
df_trans_region = df_transactions.withColumn("region_copy", col("region"))
df_cust_region = df_customers.withColumn("region_copy", lit("North"))
multi = df_trans_region.join(df_cust_region, ["customer_id", "region_copy"], "left")
print(f"   Records: {multi.count()}")

print("\n3. Complex condition join:")
# Range join example
complex = df_transactions.alias("t").join(
    df_products.alias("p"),
    (col("t.product_id") == col("p.product_id")) & 
    (col("t.amount") >= col("p.price") * 0.8),  # Allow 20% discount
    "inner"
)
print(f"   Records: {complex.count()}")

"""
Complex Join Performance:

Equality Joins:
✓ Fast (hash-based)
✓ Can use broadcast
✓ Partition pruning

Range Joins:
⚠️  Slower (nested loop or sort-merge)
⚠️  Can't always broadcast
⚠️  Consider alternatives

Optimization for Range Joins:
1. Bucket data by range
2. Use inequalities sparingly
3. Add equality conditions when possible
"""

# ============================================================================
# PART 7: Skewed Join Optimization
# ============================================================================

print("\n" + "=" * 80)
print("PART 7: HANDLING SKEWED JOINS")
print("=" * 80)

"""
Skewed Join: When join key distribution is uneven
Problem: One key has many records → slow partition
Solution: Salting technique
"""

print("\nDemonstrating skewed join optimization...")

# Create skewed data (80% records have same customer_id)
skewed_data = []
for i in range(8000):
    skewed_data.append(("CUST0001", f"TXN{i}", 100.0))
for i in range(8000, 10000):
    skewed_data.append((f"CUST{i:04d}", f"TXN{i}", 100.0))

df_skewed = spark.createDataFrame(skewed_data, ["customer_id", "txn_id", "amount"])

print("\nSkewed distribution:")
df_skewed.groupBy("customer_id").count().orderBy(col("count").desc()).show(5)

# Regular join (will be slow)
print("\nRegular join on skewed data...")
start = time.time()
regular_skewed = df_skewed.join(df_customers, "customer_id")
regular_skewed.count()
time_regular = time.time() - start
print(f"  Time: {time_regular:.4f}s")

# Optimized: Salting technique
print("\nOptimized with salting...")

# Add salt to skewed table
SALT_RANGE = 10
df_skewed_salted = df_skewed.withColumn(
    "salt",
    (rand() * SALT_RANGE).cast("int")
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))
)

# Replicate small table with salt
from pyspark.sql.functions import explode, array, lit
df_customers_replicated = df_customers.withColumn(
    "salt",
    explode(array([lit(i) for i in range(SALT_RANGE)]))
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))
)

# Join on salted key
start = time.time()
salted_join = df_skewed_salted.join(
    df_customers_replicated,
    "customer_id_salted"
).drop("customer_id_salted", "salt")
salted_join.count()
time_salted = time.time() - start

print(f"  Time: {time_salted:.4f}s")
print(f"  Improvement: {time_regular/time_salted:.2f}x faster")

"""
Salting Technique:

Problem:
- Key "CUST0001" has 8000 records
- Goes to single partition
- Slow task (straggler)

Solution:
1. Add random salt (0-9) to skewed table
   "CUST0001" → "CUST0001_0", "CUST0001_1", ..., "CUST0001_9"

2. Replicate small table 10x with all salts
   "CUST0001" → ["CUST0001_0", "CUST0001_1", ..., "CUST0001_9"]

3. Join on salted key
   - 8000 records distributed across 10 partitions
   - No single slow partition

Trade-off:
✓ Fixes skew
✓ Better parallelism
⚠️  10x larger small table (still small)
⚠️  More complex logic
"""

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. BROADCAST JOINS:
   ✓ Use broadcast() hint for small tables (<10MB)
   ✓ 2-10x faster than sort-merge
   ✓ No shuffle of large table
   ✓ Configure: spark.sql.autoBroadcastJoinThreshold

2. PRE-PARTITIONING:
   ✓ Repartition by join key before join
   ✓ Cache partitioned data
   ✓ Reuse for multiple joins
   ✓ Reduces shuffle cost

3. JOIN STRATEGIES:
   ✓ BroadcastHashJoin: Small-large tables
   ✓ SortMergeJoin: Large-large tables
   ✓ left_semi: Existence checks (efficient)
   ✓ left_anti: Find unmatched records

4. MULTI-TABLE JOINS:
   ✓ Start with largest table
   ✓ Broadcast small dimensions
   ✓ Filter early
   ✓ Optimize join order

5. SKEWED JOINS:
   ✓ Detect: Check key distribution
   ✓ Fix: Salting technique
   ✓ Enable: spark.sql.adaptive.skewJoin.enabled
   ✓ Alternative: Broadcast if small enough

6. PERFORMANCE TIPS:
   ✓ Use explain() to verify join strategy
   ✓ Check Spark UI for shuffle metrics
   ✓ Monitor task duration (detect stragglers)
   ✓ Consider data size when choosing strategy

7. CONFIGURATIONS:
   ✓ spark.sql.autoBroadcastJoinThreshold: 10MB
   ✓ spark.sql.adaptive.enabled: true
   ✓ spark.sql.adaptive.skewJoin.enabled: true
   ✓ spark.sql.shuffle.partitions: Tune based on data

8. ANTI-PATTERNS:
   ✗ Not broadcasting small tables
   ✗ Cross joins on large data
   ✗ Ignoring data skew
   ✗ Not caching reused DataFrames
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Always check join strategy in execution plan!")
