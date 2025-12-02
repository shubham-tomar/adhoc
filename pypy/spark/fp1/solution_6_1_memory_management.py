"""
Problem 6.1: Memory Management
===============================
Goal: Optimize Spark memory settings for different workloads

Key Concepts:
- Memory configuration
- Caching strategies
- Storage levels
- OOM troubleshooting
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import StorageLevel

spark = SparkSession.builder \
    .appName("Problem 6.1 - Memory Management") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 6.1: MEMORY MANAGEMENT")
print("=" * 80)

# ============================================================================
# PART 1: Understanding Spark Memory Model
# ============================================================================

print("\n" + "-" * 80)
print("PART 1: SPARK MEMORY MODEL")
print("-" * 80)

"""
Spark Memory Breakdown:

Executor Memory = Execution Memory + Storage Memory + User Memory + Reserved

1. Execution Memory (60% of heap):
   - Shuffles, joins, sorts, aggregations
   - Temporary buffers
   - Spills to disk if full

2. Storage Memory (60% of heap, shared with execution):
   - Cached DataFrames/RDDs
   - Broadcast variables
   - Can borrow from execution

3. User Memory (40% of heap):
   - User data structures
   - UDFs
   - Spark internal metadata

4. Reserved Memory (300MB):
   - System reserved

Key Configs:
- spark.executor.memory: Total executor heap
- spark.memory.fraction: 0.6 (execution + storage)
- spark.memory.storageFraction: 0.5 (within above)
"""

print("\nCurrent Memory Configuration:")
print(f"  executor.memory: {spark.conf.get('spark.executor.memory', 'default (1g)')}")
print(f"  memory.fraction: {spark.conf.get('spark.memory.fraction', '0.6')}")
print(f"  memory.storageFraction: {spark.conf.get('spark.memory.storageFraction', '0.5')}")

# ============================================================================
# PART 2: Caching Strategies
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: CACHING STRATEGIES")
print("=" * 80)

df = spark.read.parquet("transactions.parquet")

"""
Storage Levels:

MEMORY_ONLY:
✓ Fastest access
✗ OOM if data doesn't fit
✗ Lost if executor dies

MEMORY_AND_DISK:
✓ Spills to disk if memory full
✓ No data loss
⚠️  Slower disk access

MEMORY_ONLY_SER:
✓ Less memory (serialized)
⚠️  CPU overhead (deserialization)

DISK_ONLY:
✗ Slowest
✓ No memory pressure
"""

print("\n1. MEMORY_ONLY (default cache):")
df_cached_mem = df.filter(col("amount") > 100).cache()
df_cached_mem.count()  # Materialize cache

print(f"   Cached: {df_cached_mem.count()} records")
print(f"   Storage level: {df_cached_mem.storageLevel}")

# Check if cached
is_cached = spark.catalog.isCached("transactions")
print(f"   Is cached: {df_cached_mem.is_cached}")

df_cached_mem.unpersist()

print("\n2. MEMORY_AND_DISK:")
df_cached_disk = df.filter(col("amount") > 100).persist(StorageLevel.MEMORY_AND_DISK)
df_cached_disk.count()

print(f"   Storage level: {df_cached_disk.storageLevel}")
df_cached_disk.unpersist()

print("\n3. MEMORY_ONLY_SER (serialized):")
df_cached_ser = df.filter(col("amount") > 100).persist(StorageLevel.MEMORY_ONLY_SER)
df_cached_ser.count()

print(f"   Storage level: {df_cached_ser.storageLevel}")
print("   ✓ Uses less memory (serialized)")
print("   ⚠️  Slower access (deserialization)")

df_cached_ser.unpersist()

"""
When to Cache:

✓ DataFrame used multiple times
✓ Iterative algorithms (ML)
✓ Interactive analysis
✓ Expensive computations

✗ One-time use
✗ Large data that doesn't fit
✗ Simple transformations
"""

# ============================================================================
# PART 3: Cache vs Persist
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: CACHE vs PERSIST")
print("=" * 80)

"""
cache() = persist(MEMORY_AND_DISK)

When to use each:

cache():
- Quick caching with sensible default
- Good for most use cases

persist(level):
- Fine-grained control
- Specific memory/disk requirements
- Production tuning
"""

# Example: Reuse cached DataFrame
print("\nDemonstrating cache benefits:")

import time

# Without cache
print("\n1. Without cache (compute twice):")
df_nocache = df.filter(col("amount") > 100)

start = time.time()
count1 = df_nocache.count()
time1 = time.time() - start

start = time.time()
count2 = df_nocache.count()
time2 = time.time() - start

print(f"   First count: {time1:.4f}s")
print(f"   Second count: {time2:.4f}s (re-computed!)")

# With cache
print("\n2. With cache (compute once):")
df_withcache = df.filter(col("amount") > 100).cache()

start = time.time()
count1 = df_withcache.count()
time1 = time.time() - start

start = time.time()
count2 = df_withcache.count()
time2 = time.time() - start

print(f"   First count: {time1:.4f}s (computed + cached)")
print(f"   Second count: {time2:.4f}s (from cache)")
print(f"   Speedup: {time1/time2:.2f}x")

df_withcache.unpersist()

# ============================================================================
# PART 4: OOM Troubleshooting
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: OOM TROUBLESHOOTING")
print("=" * 80)

"""
Common OOM Causes:

1. collect() on large data
2. Too few partitions
3. Data skew
4. Too much caching
5. Memory-intensive UDFs

Solutions:

1. Avoid collect():
   ✗ data = df.collect()
   ✓ df.write.parquet("output")
   ✓ result = df.take(100)

2. Increase partitions:
   df.repartition(200)
   spark.conf.set("spark.sql.shuffle.partitions", "200")

3. Fix skew:
   - Salting
   - Broadcast join
   - Filter early

4. Clear cache:
   spark.catalog.clearCache()
   df.unpersist()

5. Increase memory:
   --executor-memory 4g
   --driver-memory 2g
"""

print("\nOOM Prevention Checklist:")
checklist = """
□ Avoid collect() on large DataFrames
□ Use take(N) instead of collect()
□ Increase partition count for large shuffles
□ Check for data skew
□ Unpersist unused cached DataFrames
□ Use efficient storage levels
□ Filter data early
□ Use broadcast for small tables
□ Monitor Spark UI for memory usage
□ Increase executor memory if needed
"""
print(checklist)

# ============================================================================
# PART 5: Memory Monitoring
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: MEMORY MONITORING")
print("=" * 80)

"""
Monitoring Tools:

1. Spark UI (localhost:4040):
   - Storage tab: Cached RDDs/DataFrames
   - Executors tab: Memory usage
   - SQL tab: Query metrics

2. Programmatic:
   - spark.catalog.isCached()
   - df.storageLevel
   - spark.catalog.clearCache()

3. Metrics:
   - Total cached memory
   - Spilled memory
   - Executor memory
"""

print("\nCache Management Commands:")
print("  spark.catalog.clearCache()  # Clear all cached data")
print("  df.unpersist()               # Unpersist specific DataFrame")
print("  df.persist(level)            # Cache with storage level")
print("  df.cache()                   # Cache (MEMORY_AND_DISK)")
print("  spark.catalog.isCached('t')  # Check if table cached")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. MEMORY MODEL:
   ✓ Execution: Shuffles, joins, sorts
   ✓ Storage: Cached data
   ✓ User: UDFs, metadata
   ✓ Reserved: System (300MB)

2. STORAGE LEVELS:
   MEMORY_ONLY: Fastest, OOM risk
   MEMORY_AND_DISK: Safe default
   MEMORY_ONLY_SER: Less memory, CPU overhead
   DISK_ONLY: Slowest, no memory pressure

3. CACHING BEST PRACTICES:
   ✓ Cache reused DataFrames
   ✓ Unpersist when done
   ✓ Use MEMORY_AND_DISK for production
   ✓ Monitor cache usage

4. OOM PREVENTION:
   ✗ Avoid collect() on large data
   ✓ Increase partitions
   ✓ Fix data skew
   ✓ Clear unused cache
   ✓ Use broadcast joins

5. CONFIGURATION:
   executor.memory: Executor heap size
   memory.fraction: 0.6 (60%)
   memory.storageFraction: 0.5 (50%)

6. MONITORING:
   ✓ Spark UI Storage tab
   ✓ Executors tab memory metrics
   ✓ Check for spilled memory
   ✓ Monitor GC time
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
