"""
Problem 4.3: Output Partitioning Strategy
==========================================
Goal: Control output file partitioning for different scenarios

Key Concepts:
- Partition by columns for query optimization
- Control number of output files
- File size management
- Nested partitioning schemes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Problem 4.3 - Output Partitioning") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 4.3: OUTPUT PARTITIONING STRATEGY")
print("=" * 80)

# Read and prepare data
df = spark.read.parquet("transactions.parquet")
df = df.withColumn("transaction_date", to_date(col("transaction_date")))

# Add year, month, day columns for partitioning
df = df.withColumn("year", year("transaction_date")) \
       .withColumn("month", month("transaction_date")) \
       .withColumn("day", dayofmonth("transaction_date"))

print(f"\nDataset: {df.count()} records")
print("\nSample data:")
df.show(5)

# ============================================================================
# SCENARIO 1: One File per Region
# ============================================================================

print("\n" + "=" * 80)
print("SCENARIO 1: WRITE ONE FILE PER REGION")
print("=" * 80)

"""
Use Case: Separate files for each region
- Easy to distribute to regional teams
- Can delete old regions without affecting others
- Parallel processing by region

Technique: partitionBy("region")
"""

print("\nWriting one file per region...")

output_path_1 = "output/scenario_1_by_region"

# Method 1: Repartition by region first (ensures one partition per region)
df.repartition(1, "region") \
  .write \
  .mode("overwrite") \
  .partitionBy("region") \
  .parquet(output_path_1)

print(f"✓ Written to: {output_path_1}")

# Check output structure
print("\nOutput directory structure:")
import os
for root, dirs, files in os.walk(output_path_1):
    level = root.replace(output_path_1, '').count(os.sep)
    indent = ' ' * 2 * level
    print(f'{indent}{os.path.basename(root)}/')
    subindent = ' ' * 2 * (level + 1)
    for file in files[:3]:  # Show first 3 files
        print(f'{subindent}{file}')
    if len(files) > 3:
        print(f'{subindent}... and {len(files)-3} more files')
    if level > 2:  # Limit depth
        break

"""
Directory Structure:

output/scenario_1_by_region/
├── region=North/
│   └── part-00000-xxx.parquet
├── region=South/
│   └── part-00000-xxx.parquet
├── region=East/
│   └── part-00000-xxx.parquet
└── region=West/
    └── part-00000-xxx.parquet

Benefits:
✓ One file per region (easy to manage)
✓ Partition pruning when filtering by region
✓ Can process regions independently

Drawbacks:
⚠️  Small files if regions have few records
⚠️  Requires repartition(1, "region") for single file
"""

# Read back with partition pruning
print("\nReading back with partition filter:")
df_north = spark.read.parquet(output_path_1).filter(col("region") == "North")
print(f"  Records from North region: {df_north.count()}")

# Check physical plan for partition pruning
print("\nPhysical plan (should show partition filter):")
df_north.explain()

# ============================================================================
# SCENARIO 2: Exactly 5 Evenly-Sized Files
# ============================================================================

print("\n" + "=" * 80)
print("SCENARIO 2: WRITE EXACTLY 5 FILES")
print("=" * 80)

"""
Use Case: Control number of output files
- Downstream system expects N files
- Balance between parallelism and file count
- Even distribution of data

Technique: coalesce(N) or repartition(N)
"""

print("\nWriting exactly 5 evenly-sized files...")

output_path_2 = "output/scenario_2_five_files"

# Option 1: coalesce (faster, may be uneven)
df.coalesce(5) \
  .write \
  .mode("overwrite") \
  .parquet(output_path_2 + "_coalesce")

print(f"✓ Coalesce method: {output_path_2}_coalesce")

# Option 2: repartition (slower, but even)
df.repartition(5) \
  .write \
  .mode("overwrite") \
  .parquet(output_path_2 + "_repartition")

print(f"✓ Repartition method: {output_path_2}_repartition")

# Compare file sizes
print("\nComparing file sizes...")

def get_file_sizes(path):
    """Get sizes of parquet files in directory"""
    sizes = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                sizes.append(os.path.getsize(file_path))
    return sizes

try:
    sizes_coalesce = get_file_sizes(output_path_2 + "_coalesce")
    sizes_repartition = get_file_sizes(output_path_2 + "_repartition")
    
    print(f"\nCoalesce method:")
    print(f"  Files: {len(sizes_coalesce)}")
    print(f"  Sizes: {[f'{s/1024:.1f}KB' for s in sizes_coalesce]}")
    print(f"  Min/Max ratio: {max(sizes_coalesce)/min(sizes_coalesce):.2f}x")
    
    print(f"\nRepartition method:")
    print(f"  Files: {len(sizes_repartition)}")
    print(f"  Sizes: {[f'{s/1024:.1f}KB' for s in sizes_repartition]}")
    print(f"  Min/Max ratio: {max(sizes_repartition)/min(sizes_repartition):.2f}x")
except:
    print("  (File size comparison not available)")

"""
coalesce vs repartition for output:

coalesce(5):
✓ Faster (no full shuffle)
✓ Good for reducing partitions
⚠️  May create uneven files

repartition(5):
✓ Even file sizes
⚠️  Slower (full shuffle)
✓ Better for even distribution

Recommendation:
- Use repartition for production (even files)
- Use coalesce for ad-hoc exports (faster)
"""

# ============================================================================
# SCENARIO 3: Files No Larger Than 128MB
# ============================================================================

print("\n" + "=" * 80)
print("SCENARIO 3: CONTROL MAXIMUM FILE SIZE")
print("=" * 80)

"""
Use Case: Limit file size for:
- HDFS block size alignment
- S3 multipart upload optimization
- Avoid too-large files

Technique: maxRecordsPerFile option
"""

print("\nWriting with file size limit...")

output_path_3 = "output/scenario_3_size_limit"

# Calculate records per file to stay under 128MB
# Assume ~1KB per record (adjust based on actual data)
BYTES_PER_RECORD = 1000
MAX_FILE_SIZE_MB = 128
MAX_RECORDS_PER_FILE = int((MAX_FILE_SIZE_MB * 1024 * 1024) / BYTES_PER_RECORD)

print(f"  Target max file size: {MAX_FILE_SIZE_MB}MB")
print(f"  Max records per file: {MAX_RECORDS_PER_FILE:,}")

df.write \
  .mode("overwrite") \
  .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE) \
  .parquet(output_path_3)

print(f"✓ Written to: {output_path_3}")

# Count output files
try:
    files = [f for f in os.listdir(output_path_3) if f.endswith('.parquet')]
    print(f"\nOutput files created: {len(files)}")
    
    # Check sizes
    sizes = get_file_sizes(output_path_3)
    if sizes:
        print(f"  Largest file: {max(sizes)/1024/1024:.2f}MB")
        print(f"  Average file: {sum(sizes)/len(sizes)/1024/1024:.2f}MB")
except:
    print("  (File count not available)")

"""
maxRecordsPerFile Configuration:

Benefits:
✓ Control file size precisely
✓ Prevent too-large files
✓ Better for systems with file size limits

Limitations:
⚠️  Need to estimate bytes per record
⚠️  May create many small files
⚠️  Can't guarantee exact size (compression varies)

Alternative:
- Use maxFileSize option (Spark 3.0+)
  .option("maxFileSize", "128MB")
"""

# ============================================================================
# SCENARIO 4: Nested Partitioning (Date and Region)
# ============================================================================

print("\n" + "=" * 80)
print("SCENARIO 4: NESTED PARTITIONING (YEAR/MONTH/DAY + REGION)")
print("=" * 80)

"""
Use Case: Time-series data with multiple dimensions
- Query by date range
- Filter by region within date
- Efficient partition pruning

Technique: partitionBy multiple columns
"""

print("\nWriting with nested partitioning...")

output_path_4 = "output/scenario_4_nested"

# Method 1: Date hierarchy only
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .parquet(output_path_4 + "_date_only")

print(f"✓ Date hierarchy: {output_path_4}_date_only")

# Method 2: Date + Region
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month", "day", "region") \
  .parquet(output_path_4 + "_date_region")

print(f"✓ Date + Region: {output_path_4}_date_region")

# Method 3: Region first, then date (different query pattern)
df.write \
  .mode("overwrite") \
  .partitionBy("region", "year", "month") \
  .parquet(output_path_4 + "_region_date")

print(f"✓ Region + Date: {output_path_4}_region_date")

print("\nDirectory structures created:")
print("\n1. Date hierarchy:")
print("   year=2024/month=1/day=15/")
print("   year=2024/month=1/day=16/")

print("\n2. Date + Region:")
print("   year=2024/month=1/day=15/region=North/")
print("   year=2024/month=1/day=15/region=South/")

print("\n3. Region + Date:")
print("   region=North/year=2024/month=1/")
print("   region=South/year=2024/month=1/")

"""
Partition Order Matters!

Choose based on query patterns:

partitionBy("year", "month", "day"):
✓ Good for: SELECT * WHERE year=2024 AND month=1
✗ Bad for: SELECT * WHERE region='North'

partitionBy("region", "year", "month"):
✓ Good for: SELECT * WHERE region='North'
✓ Good for: SELECT * WHERE region='North' AND year=2024
✗ Less efficient for date-only queries

Partition Pruning Rules:
- Can only prune on prefix of partition columns
- Example: partitionBy("a", "b", "c")
  ✓ WHERE a=1 AND b=2: Prunes on a,b
  ✓ WHERE a=1: Prunes on a
  ✗ WHERE b=2: No pruning (a not filtered)
"""

# Demonstrate partition pruning
print("\nDemonstrating partition pruning...")

df_date_region = spark.read.parquet(output_path_4 + "_date_region")

# Query 1: Date range (efficient)
print("\n1. Query by date range:")
filtered_1 = df_date_region.filter(
    (col("year") == 2024) & (col("month") == 1)
)
print(f"   Records: {filtered_1.count()}")
print("   Plan (should show partition filter):")
filtered_1.explain()

# Query 2: Date + Region (very efficient)
print("\n2. Query by date AND region:")
filtered_2 = df_date_region.filter(
    (col("year") == 2024) & (col("month") == 1) & (col("region") == "North")
)
print(f"   Records: {filtered_2.count()}")

# ============================================================================
# SCENARIO 5: Dynamic Partition Overwrite
# ============================================================================

print("\n" + "=" * 80)
print("SCENARIO 5: DYNAMIC PARTITION OVERWRITE")
print("=" * 80)

"""
Use Case: Update specific partitions without affecting others
- Reprocess failed partitions
- Incremental updates
- Keep other partitions intact

Technique: Dynamic partition overwrite mode
"""

print("\nDemonstrating dynamic partition overwrite...")

output_path_5 = "output/scenario_5_dynamic"

# Initial write
print("\n1. Initial write (all regions):")
df.write \
  .mode("overwrite") \
  .partitionBy("region") \
  .parquet(output_path_5)

initial_regions = spark.read.parquet(output_path_5).select("region").distinct().count()
print(f"   Regions written: {initial_regions}")

# Update only North region
print("\n2. Update only North region (dynamic overwrite):")
df_north_updated = df.filter(col("region") == "North") \
                     .withColumn("amount", col("amount") * 1.1)  # 10% increase

# Enable dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_north_updated.write \
  .mode("overwrite") \
  .partitionBy("region") \
  .parquet(output_path_5)

print("   ✓ Only North partition overwritten")

# Verify
print("\n3. Verification:")
final_regions = spark.read.parquet(output_path_5).select("region").distinct().count()
print(f"   Regions after update: {final_regions}")
print(f"   ✓ Other regions preserved: {final_regions == initial_regions}")

# Reset to default
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

"""
Partition Overwrite Modes:

STATIC (default):
- mode("overwrite") deletes ALL partitions
- Then writes new data
- Dangerous for partial updates

DYNAMIC:
- mode("overwrite") deletes ONLY partitions in new data
- Other partitions preserved
- Safe for incremental updates

Enable:
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

Use Case Example:
# Reprocess yesterday's data without affecting other days
df_yesterday.write
  .mode("overwrite")
  .partitionBy("date")
  .parquet("output/daily_data")
# Only yesterday's partition overwritten!
"""

# ============================================================================
# SCENARIO 6: Bucketing (Advanced)
# ============================================================================

print("\n" + "=" * 80)
print("SCENARIO 6: BUCKETING")
print("=" * 80)

"""
Bucketing: Pre-shuffle data into buckets
- Faster joins on bucketed column
- Sorted buckets for range queries
- More advanced than partitioning
"""

print("\nWriting bucketed table...")

output_path_6 = "output/scenario_6_bucketed"

# Create bucketed table
df.write \
  .mode("overwrite") \
  .bucketBy(10, "customer_id") \
  .sortBy("transaction_date") \
  .saveAsTable("transactions_bucketed", path=output_path_6, format="parquet")

print(f"✓ Bucketed table created")
print("   Buckets: 10")
print("   Bucket column: customer_id")
print("   Sort column: transaction_date")

"""
Bucketing Benefits:

1. Join Optimization:
   - Join two bucketed tables on bucket column
   - No shuffle needed!
   - Much faster

2. Sorted Buckets:
   - Range queries optimized
   - Skip buckets outside range

3. Partition + Bucket:
   - Can combine: partitionBy + bucketBy
   - Example: partition by date, bucket by user_id

Limitations:
⚠️  Only works with saveAsTable (managed tables)
⚠️  More complex than partitioning
⚠️  Need to choose bucket count carefully

Bucket Count Guidelines:
- Too few: Large buckets, less parallelism
- Too many: Small buckets, overhead
- Sweet spot: 50-200 buckets for large tables
"""

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. FILE COUNT CONTROL:
   ✓ coalesce(N): Reduce to N files (fast, may be uneven)
   ✓ repartition(N): Exactly N files (even distribution)
   ✓ repartition(1, col): One file per unique value

2. FILE SIZE CONTROL:
   ✓ maxRecordsPerFile: Limit records per file
   ✓ maxFileSize: Limit file size (Spark 3.0+)
   ✓ Useful for HDFS block alignment

3. PARTITIONING SCHEMES:
   ✓ partitionBy(col): Create directory per value
   ✓ partitionBy("year", "month"): Nested hierarchy
   ✓ Order matters for partition pruning

4. PARTITION PRUNING:
   ✓ Works on prefix of partition columns
   ✓ WHERE year=2024 AND month=1: Efficient
   ✗ WHERE month=1: No pruning (year not filtered)

5. DYNAMIC PARTITIONS:
   ✓ Set partitionOverwriteMode=dynamic
   ✓ Overwrites only affected partitions
   ✓ Safe for incremental updates

6. BUCKETING:
   ✓ Pre-shuffle by column
   ✓ Faster joins on bucket column
   ✓ Use with saveAsTable
   ✓ Combine with partitioning

7. CHOOSING STRATEGY:
   Small data → Single file: coalesce(1)
   Medium data → Few files: repartition(5-10)
   Large data → Partitioned: partitionBy("date")
   Very large → Partitioned + Bucketed

8. BEST PRACTICES:
   ✓ Partition by low-cardinality columns (date, region)
   ✗ Don't partition by high-cardinality (user_id)
   ✓ Aim for 128MB-1GB files
   ✓ Use dynamic overwrite for updates
   ✓ Test partition pruning with explain()

9. ANTI-PATTERNS:
   ✗ Too many partitions (>10,000)
   ✗ Too many small files (<10MB)
   ✗ Partitioning by unique columns
   ✗ Not testing partition pruning
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Good partitioning = Faster queries + Lower costs!")
