"""
Problem 7.3: Performance Troubleshooting
=========================================
Goal: Debug and optimize a slow Spark job

Key Concepts:
- Identifying performance bottlenecks
- Spark UI interpretation
- Common anti-patterns
- Optimization techniques
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Problem 7.3 - Performance Troubleshooting") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 7.3: PERFORMANCE TROUBLESHOOTING")
print("=" * 80)

# ============================================================================
# PART 1: Intentionally Inefficient Code
# ============================================================================

print("\n" + "=" * 80)
print("PART 1: INEFFICIENT CODE (ANTI-PATTERNS)")
print("=" * 80)

def slow_operation():
    """Intentionally inefficient implementation"""
    
    df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
    
    # ANTI-PATTERN 1: Multiple passes over same data
    print("\n❌ ANTI-PATTERN 1: Multiple passes")
    high_value = df.filter(col("amount") > 500).count()
    low_value = df.filter(col("amount") <= 100).count()
    medium_value = df.filter((col("amount") > 100) & (col("amount") <= 500)).count()
    
    print(f"   High: {high_value}, Medium: {medium_value}, Low: {low_value}")
    print("   Problem: 3 full scans of data!")
    
    # ANTI-PATTERN 2: Inefficient join
    print("\n❌ ANTI-PATTERN 2: Inefficient self-join")
    df1 = df.select("customer_id", "amount")
    df2 = df.select("customer_id", "category")
    result = df1.join(df2, "customer_id")
    
    print(f"   Result: {result.count()}")
    print("   Problem: Self-join instead of single select!")
    
    # ANTI-PATTERN 3: collect() on large data
    print("\n❌ ANTI-PATTERN 3: Collecting large data")
    try:
        # all_data = result.collect()  # DON'T DO THIS!
        print("   (Skipped to avoid OOM)")
        print("   Problem: Brings all data to driver!")
    except:
        pass
    
    # ANTI-PATTERN 4: Not caching reused DataFrame
    print("\n❌ ANTI-PATTERN 4: No caching")
    for i in range(3):
        df.filter(col("amount") > 100).count()
    print("   Problem: Recomputes same DataFrame 3 times!")
    
    # ANTI-PATTERN 5: Unnecessary repartition
    print("\n❌ ANTI-PATTERN 5: Too many repartitions")
    df.repartition(100).repartition(50).repartition(10).count()
    print("   Problem: 3 shuffles when 1 would suffice!")

print("\nRunning inefficient code...")
start = time.time()
slow_operation()
time_slow = time.time() - start
print(f"\n⏱️  Inefficient version: {time_slow:.4f}s")

# ============================================================================
# PART 2: Optimized Code
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: OPTIMIZED CODE")
print("=" * 80)

def fast_operation():
    """Optimized implementation"""
    
    df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
    
    # FIX 1: Single pass with aggregation
    print("\n✅ FIX 1: Single pass aggregation")
    value_counts = df.groupBy().agg(
        sum(when(col("amount") > 500, 1).otherwise(0)).alias("high_value"),
        sum(when(col("amount") <= 100, 1).otherwise(0)).alias("low_value"),
        sum(when((col("amount") > 100) & (col("amount") <= 500), 1).otherwise(0)).alias("medium_value")
    ).collect()[0]
    
    print(f"   High: {value_counts['high_value']}, Medium: {value_counts['medium_value']}, Low: {value_counts['low_value']}")
    print("   ✓ Single scan of data")
    
    # FIX 2: Single select instead of join
    print("\n✅ FIX 2: Single select")
    result = df.select("customer_id", "amount", "category")
    
    print(f"   Result: {result.count()}")
    print("   ✓ No unnecessary join")
    
    # FIX 3: Use take() instead of collect()
    print("\n✅ FIX 3: Use take() for samples")
    sample = result.take(10)
    print(f"   Sampled {len(sample)} rows")
    print("   ✓ Only fetches needed rows")
    
    # FIX 4: Cache reused DataFrame
    print("\n✅ FIX 4: Cache reused DataFrame")
    cached_df = df.filter(col("amount") > 100).cache()
    for i in range(3):
        cached_df.count()
    cached_df.unpersist()
    print("   ✓ Computed once, reused 3 times")
    
    # FIX 5: Single repartition
    print("\n✅ FIX 5: Single repartition")
    df.repartition(10).count()
    print("   ✓ Single shuffle")

print("\nRunning optimized code...")
start = time.time()
fast_operation()
time_fast = time.time() - start
print(f"\n⏱️  Optimized version: {time_fast:.4f}s")
print(f"🚀 Speedup: {time_slow/time_fast:.2f}x faster")

# ============================================================================
# PART 3: Spark UI Interpretation
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: SPARK UI INTERPRETATION")
print("=" * 80)

"""
Spark UI (localhost:4040):

1. JOBS TAB:
   - Duration: Total job time
   - Stages: Number of stages (shuffles)
   - Tasks: Parallel tasks executed
   
   Red Flags:
   ⚠️  Long duration
   ⚠️  Many stages (multiple shuffles)
   ⚠️  Uneven task distribution

2. STAGES TAB:
   - Task metrics
   - Shuffle read/write
   - Spill metrics
   - GC time
   
   Red Flags:
   ⚠️  High shuffle write/read
   ⚠️  Spilled data (memory pressure)
   ⚠️  GC time > 10% of task time
   ⚠️  Long stragglers (skew)

3. STORAGE TAB:
   - Cached RDDs/DataFrames
   - Memory usage
   - Disk usage
   
   Red Flags:
   ⚠️  Not using cache when should
   ⚠️  Too much cached data (eviction)
   ⚠️  Spilled to disk

4. EXECUTORS TAB:
   - Executor memory/CPU
   - Task distribution
   - Failures
   
   Red Flags:
   ⚠️  OOM errors
   ⚠️  Uneven task distribution
   ⚠️  Failed tasks

5. SQL TAB:
   - Query execution plan
   - Physical plan metrics
   - Shuffle details
   
   Red Flags:
   ⚠️  Missing predicate pushdown
   ⚠️  No column pruning
   ⚠️  Broadcast join missed
"""

print("\nSpark UI Checklist:")
checklist = """
□ Check job duration (Jobs tab)
□ Count stages (minimize shuffles)
□ Look for stragglers (skew)
□ Check shuffle size (Stages tab)
□ Monitor spilled data
□ Check GC time (< 10%)
□ Verify cached data (Storage tab)
□ Check executor utilization
□ Review SQL query plan
□ Verify predicate pushdown
□ Confirm column pruning
□ Check broadcast join eligibility
"""
print(checklist)

# ============================================================================
# PART 4: Common Performance Issues
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: COMMON PERFORMANCE ISSUES & FIXES")
print("=" * 80)

issues_and_fixes = """
┌──────────────────────┬─────────────────────┬──────────────────────┐
│ Issue                │ Symptom             │ Fix                  │
├──────────────────────┼─────────────────────┼──────────────────────┤
│ Data Skew            │ Stragglers          │ Salting, repartition │
│ Too Many Shuffles    │ Slow, many stages   │ Combine operations   │
│ Small Files          │ Many tasks          │ Coalesce             │
│ Large Partitions     │ OOM errors          │ Increase partitions  │
│ Not Caching          │ Recomputation       │ cache() reused DFs   │
│ Wrong Join Strategy  │ Slow joins          │ Broadcast small table│
│ No Predicate Pushdown│ Reading all data    │ Filter early         │
│ High GC Time         │ Memory pressure     │ Increase executor mem│
│ Spilled Data         │ Disk I/O            │ Increase memory      │
│ Unused Broadcast     │ Large shuffle       │ broadcast() hint     │
└──────────────────────┴─────────────────────┴──────────────────────┘
"""
print(issues_and_fixes)

# ============================================================================
# PART 5: Debugging Workflow
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: DEBUGGING WORKFLOW")
print("=" * 80)

debugging_steps = """
Step 1: IDENTIFY THE PROBLEM
  □ Check Spark UI for slow stages
  □ Look for stragglers (data skew)
  □ Check shuffle sizes
  □ Monitor memory usage

Step 2: ANALYZE ROOT CAUSE
  □ Review execution plan (explain())
  □ Check partition distribution
  □ Verify caching strategy
  □ Look for anti-patterns

Step 3: HYPOTHESIS
  □ Formulate optimization theory
  □ Identify specific bottleneck
  □ Estimate impact

Step 4: IMPLEMENT FIX
  □ Apply single optimization
  □ Measure impact
  □ Document change

Step 5: VERIFY
  □ Rerun with Spark UI monitoring
  □ Compare before/after metrics
  □ Check for new issues

Step 6: ITERATE
  □ Move to next bottleneck
  □ Repeat process
  □ Document optimizations
"""
print(debugging_steps)

# ============================================================================
# PART 6: Optimization Techniques
# ============================================================================

print("\n" + "=" * 80)
print("PART 6: OPTIMIZATION TECHNIQUES")
print("=" * 80)

# 1. Filter Early
print("\n1. FILTER EARLY:")
print("   ❌ df.join(other).filter(cond)")
print("   ✅ df.filter(cond).join(other.filter(cond2))")

# 2. Broadcast Small Tables
print("\n2. BROADCAST SMALL TABLES:")
print("   ❌ df.join(small_df, 'key')")
print("   ✅ df.join(broadcast(small_df), 'key')")

# 3. Combine Aggregations
print("\n3. COMBINE AGGREGATIONS:")
print("   ❌ df.groupBy('a').count(); df.groupBy('a').sum('b')")
print("   ✅ df.groupBy('a').agg(count('*'), sum('b'))")

# 4. Cache Appropriately
print("\n4. CACHE APPROPRIATELY:")
print("   ❌ Never caching reused DataFrames")
print("   ✅ df.cache() for iterative operations")

# 5. Partition Tuning
print("\n5. PARTITION TUNING:")
print("   ❌ Default 200 shuffle partitions")
print("   ✅ Tune based on data size")

# 6. Avoid UDFs (Python)
print("\n6. AVOID PYTHON UDFs:")
print("   ❌ Python UDF (slow serialization)")
print("   ✅ Built-in functions or Scala UDF")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. ANTI-PATTERNS:
   ✗ Multiple passes over data
   ✗ Self-joins instead of select
   ✗ collect() on large data
   ✗ Not caching reused DataFrames
   ✗ Multiple repartitions

2. OPTIMIZATION TECHNIQUES:
   ✓ Single pass aggregations
   ✓ Filter early
   ✓ Broadcast small tables
   ✓ Cache reused DataFrames
   ✓ Minimize shuffles

3. SPARK UI MONITORING:
   ✓ Jobs: Duration, stages
   ✓ Stages: Shuffle, spill, GC
   ✓ Storage: Cache usage
   ✓ Executors: Resource usage
   ✓ SQL: Query plans

4. DEBUGGING WORKFLOW:
   1. Identify problem (UI)
   2. Analyze root cause
   3. Formulate hypothesis
   4. Implement fix
   5. Verify improvement
   6. Iterate

5. COMMON FIXES:
   ✓ Skew → Salting
   ✓ Shuffles → Combine ops
   ✓ Small files → Coalesce
   ✓ OOM → Increase partitions/memory
   ✓ Slow joins → Broadcast

6. BEST PRACTICES:
   ✓ Always check Spark UI
   ✓ Use explain() liberally
   ✓ Monitor metrics
   ✓ Test optimizations
   ✓ Document changes
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Always profile before optimizing!")
print("📊 Spark UI is your best friend!")
