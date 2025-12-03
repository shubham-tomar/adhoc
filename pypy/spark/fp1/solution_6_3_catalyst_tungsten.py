"""
Problem 6.3: Catalyst Optimizer and Tungsten
=============================================
Goal: Understand and leverage Spark's optimization engines

Key Concepts:
- Catalyst optimizer (logical/physical planning)
- Tungsten engine (code generation)
- Query optimization verification
- DataFrame vs RDD performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 6.3 - Catalyst and Tungsten") \
    .config("spark.sql.codegen.wholeStage", "true") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 6.3: CATALYST OPTIMIZER AND TUNGSTEN ENGINE")
print("=" * 80)

df = spark.read.parquet("transactions.parquet")

# ============================================================================
# PART 1: Catalyst Optimizer
# ============================================================================

print("\n" + "-" * 80)
print("PART 1: CATALYST OPTIMIZER")
print("-" * 80)

"""
Catalyst: Rule-based query optimizer

Phases:
1. Analysis: Resolve column names, types
2. Logical Optimization: Apply rules
3. Physical Planning: Choose best strategy
4. Code Generation: Generate Java bytecode

Rules:
- Predicate pushdown
- Column pruning
- Constant folding
- Filter combining
- Join reordering
"""

# Example: Filter combining
print("\nExample: Multiple filters combined")

query = df.filter(col("amount") > 100) \
          .filter(col("region") == "North") \
          .filter(col("category") == "Electronics")

print("\nLogical Plan:")
query.explain(extended=True)

"""
Catalyst will combine:
  filter(amount > 100)
  filter(region = North)
  filter(category = Electronics)

Into single filter:
  filter((amount > 100) AND (region = North) AND (category = Electronics))

Benefits:
✓ Single pass over data
✓ Less intermediate data
✓ Better optimization
"""

# ============================================================================
# PART 2: Optimization Examples
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: CATALYST OPTIMIZATIONS")
print("=" * 80)

# 1. Predicate Pushdown
print("\n1. PREDICATE PUSHDOWN")
print("   Filters pushed to data source")

filtered = df.filter(col("amount") > 500)
print("\n   Physical Plan:")
filtered.explain()

print("   Look for: PushedFilters")

# 2. Column Pruning
print("\n2. COLUMN PRUNING")
print("   Only required columns read")

projected = df.select("transaction_id", "amount")
print("\n   Physical Plan:")
projected.explain()

print("   Look for: ReadSchema with only 2 columns")

# 3. Constant Folding
print("\n3. CONSTANT FOLDING")
print("   Compile-time computation")

optimized = df.withColumn("tax", col("amount") * 0.1) \
              .withColumn("total", col("amount") + (col("amount") * 0.1))

# Catalyst simplifies: amount + (amount * 0.1) → amount * 1.1
print("\n   Catalyst simplifies expressions at compile time")

# ============================================================================
# PART 3: Physical Plan Selection
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: PHYSICAL PLAN SELECTION")
print("=" * 80)

"""
Catalyst generates multiple physical plans and chooses best:

For joins:
- BroadcastHashJoin (small table)
- SortMergeJoin (large tables)
- ShuffledHashJoin (specific cases)

Cost-based optimizer (CBO):
- Uses statistics
- Estimates cost
- Chooses optimal plan
"""

# Example: Join strategy
import random
customers = [(f"CUST{i:04d}", f"Name_{i}") for i in range(1, 101)]
df_small = spark.createDataFrame(customers, ["customer_id", "name"])

print("\nJoin with small table:")
joined = df.join(df_small, "customer_id")

print("\nPhysical Plan:")
joined.explain()

print("\nLook for join strategy:")
print("  BroadcastHashJoin = Optimal for small table")
print("  SortMergeJoin = Used for large tables")

# ============================================================================
# PART 4: Tungsten Engine
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: TUNGSTEN ENGINE")
print("=" * 80)

"""
Tungsten: Execution engine optimizations

Features:
1. Whole-Stage Code Generation
   - Fuses operators
   - Generates Java bytecode
   - Eliminates virtual function calls

2. Memory Management
   - Off-heap memory
   - Binary format
   - Cache-friendly layout

3. Cache-Aware Computation
   - CPU cache optimization
   - SIMD operations

Enable:
spark.sql.codegen.wholeStage = true (default)
"""

print("\nTungsten Configuration:")
print(f"  wholeStage codegen: {spark.conf.get('spark.sql.codegen.wholeStage')}")

# Test with/without code generation
print("\n1. WITH Code Generation:")
spark.conf.set("spark.sql.codegen.wholeStage", "true")

start = time.time()
result_with = df.filter(col("amount") > 100) \
                .select("customer_id", "amount") \
                .groupBy("customer_id") \
                .sum("amount") \
                .count()
time_with = time.time() - start

print(f"   Time: {time_with:.4f}s")

print("\n2. WITHOUT Code Generation:")
spark.conf.set("spark.sql.codegen.wholeStage", "false")

start = time.time()
result_without = df.filter(col("amount") > 100) \
                   .select("customer_id", "amount") \
                   .groupBy("customer_id") \
                   .sum("amount") \
                   .count()
time_without = time.time() - start

print(f"   Time: {time_without:.4f}s")
print(f"   Speedup: {time_without/time_with:.2f}x")

# Reset
spark.conf.set("spark.sql.codegen.wholeStage", "true")

"""
Whole-Stage Code Generation:

Without:
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Filter  │ →  │ Project  │ →  │ GroupBy  │
└─────────┘     └─────────┘     └─────────┘
  Iterator      Iterator       Iterator
  (virtual calls, slow)

With:
┌──────────────────────────────────┐
│  Fused pipeline (generated code) │
│  filter + project + groupBy      │
└──────────────────────────────────┘
  (Direct calls, fast)

Benefits:
✓ 2-10x faster
✓ No virtual function overhead
✓ CPU-friendly code
"""

# ============================================================================
# PART 5: DataFrame vs RDD
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: DATAFRAME vs RDD PERFORMANCE")
print("=" * 80)

"""
DataFrame API benefits:
- Catalyst optimization
- Tungsten code generation
- Columnar processing
- Type safety at compile time

RDD API:
- No optimization
- Python/JVM overhead
- Row-by-row processing
- Flexible but slow
"""

# DataFrame approach
print("\n1. DataFrame API (Optimized):")
start = time.time()
df_result = df.filter(col("amount") > 100) \
              .groupBy("category") \
              .agg(sum("amount").alias("total")) \
              .count()
time_df = time.time() - start

print(f"   Time: {time_df:.4f}s")
print("   ✓ Catalyst optimized")
print("   ✓ Code generation")

# RDD approach (not recommended)
print("\n2. RDD API (Unoptimized):")
start = time.time()
rdd_result = df.rdd \
               .filter(lambda row: row.amount > 100) \
               .map(lambda row: (row.category, row.amount)) \
               .reduceByKey(lambda a, b: a + b) \
               .count()
time_rdd = time.time() - start

print(f"   Time: {time_rdd:.4f}s")
print("   ✗ No Catalyst")
print("   ✗ No code generation")
print(f"\nDataFrame is {time_rdd/time_df:.2f}x faster")

"""
When to use each:

DataFrame/Dataset:
✓ Structured data
✓ SQL-like operations
✓ Performance critical
✓ Most use cases (95%+)

RDD:
✓ Unstructured data
✓ Custom partitioning
✓ Low-level control
✓ Legacy code

Recommendation: Always use DataFrame API unless you have a specific reason not to
"""

# ============================================================================
# PART 6: Query Hints
# ============================================================================

print("\n" + "=" * 80)
print("PART 6: QUERY HINTS")
print("=" * 80)

"""
Hints to guide Catalyst:

1. BROADCAST: Force broadcast join
2. MERGE: Force sort-merge join
3. SHUFFLE_HASH: Force shuffle hash join
4. COALESCE/REPARTITION: Partition hints
"""

# Force broadcast join
print("\n1. BROADCAST hint:")
broadcasted = df.join(broadcast(df_small), "customer_id")
print("   df.join(broadcast(small_df), 'key')")
broadcasted.explain()

# Repartition hint
print("\n2. REPARTITION hint:")
repartitioned = df.hint("repartition", 10, "category")
print("   df.hint('repartition', 10, 'category')")

# Coalesce hint
print("\n3. COALESCE hint:")
coalesced = df.hint("coalesce", 5)
print("   df.hint('coalesce', 5')")

"""
Use hints sparingly:
✓ Catalyst usually chooses well
✓ Use when you have domain knowledge
⚠️  Can hurt performance if wrong
"""

# ============================================================================
# PART 7: Explain Modes
# ============================================================================

print("\n" + "=" * 80)
print("PART 7: UNDERSTANDING EXPLAIN()")
print("=" * 80)

query = df.filter(col("amount") > 100).groupBy("category").count()

print("\n1. Simple (physical plan):")
query.explain()

print("\n2. Extended (all plans):")
query.explain(extended=True)

print("\n3. Formatted (tree structure):")
query.explain(mode="formatted")

print("\n4. Cost (with statistics):")
query.explain(mode="cost")

"""
Explain Modes:

simple: Physical plan only
extended: Parsed, analyzed, optimized, physical
formatted: Tree structure
cost: With cost/statistics

Key sections:
- Parsed: Raw query
- Analyzed: Column resolution
- Optimized: After Catalyst rules
- Physical: Execution plan
"""

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. CATALYST OPTIMIZER:
   ✓ Logical optimization (rule-based)
   ✓ Physical planning (cost-based)
   ✓ Predicate pushdown
   ✓ Column pruning
   ✓ Constant folding

2. TUNGSTEN ENGINE:
   ✓ Whole-stage code generation
   ✓ Off-heap memory management
   ✓ Cache-friendly layout
   ✓ 2-10x performance improvement

3. DATAFRAME vs RDD:
   ✓ DataFrame: Optimized, fast
   ✗ RDD: No optimization, slow
   ✓ Always use DataFrame API

4. OPTIMIZATION RULES:
   ✓ Filter pushdown
   ✓ Filter combining
   ✓ Projection pruning
   ✓ Join reordering
   ✓ Constant folding

5. QUERY HINTS:
   ✓ broadcast(): Force broadcast join
   ✓ hint(): Guide optimizer
   ✓ Use sparingly

6. VERIFICATION:
   ✓ explain(): Check plan
   ✓ Look for "Exchange" (shuffle)
   ✓ Verify pushdown/pruning
   ✓ Check join strategy

7. BEST PRACTICES:
   ✓ Use DataFrame API
   ✓ Enable code generation
   ✓ Let Catalyst optimize
   ✓ Verify with explain()
   ✓ Use hints when needed
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Catalyst + Tungsten = Free performance!")
