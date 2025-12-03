"""
Problem 5.3: Advanced Output Control
=====================================
Goal: Implement bucketing and optimize for downstream queries

Key Concepts:
- Bucketing for join optimization
- Sorting within buckets
- MaxRecordsPerFile configuration
- Managed vs external tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Problem 5.3 - Advanced Output Control") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 5.3: ADVANCED OUTPUT CONTROL")
print("=" * 80)

# Prepare data
df = spark.read.parquet("transactions.parquet")
print(f"\nDataset: {df.count()} records")

# ============================================================================
# TASK 1: Bucketed Table
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: BUCKETED TABLE")
print("=" * 80)

"""
Bucketing:
- Pre-shuffle data into fixed number of buckets
- Based on hash of bucket column
- Optimizes joins and aggregations
- Only works with saveAsTable (managed tables)
"""

print("\nCreating bucketed table...")

bucket_path = "output/bucketed_transactions"

# Drop table if exists
spark.sql("DROP TABLE IF EXISTS transactions_bucketed")

# Create bucketed table
df.write \
  .mode("overwrite") \
  .bucketBy(10, "customer_id") \
  .sortBy("transaction_date") \
  .saveAsTable(
    "transactions_bucketed",
    format="parquet",
    path=bucket_path
  )

print(f"✓ Bucketed table created")
print("  Buckets: 10")
print("  Bucket column: customer_id")
print("  Sort column: transaction_date")

"""
Bucketing Benefits:

1. Join Optimization:
   - Join two bucketed tables on bucket column
   - No shuffle needed!
   - Same bucket number on both sides matches

2. Aggregation Optimization:
   - groupBy on bucket column
   - Less shuffle (data pre-grouped)

3. Sorted Buckets:
   - sortBy for ordering within bucket
   - Faster range queries
   - Better compression

Directory Structure:
bucket_path/
├── part-00000-xxx_00000.c000.snappy.parquet  ← Bucket 0
├── part-00000-xxx_00001.c000.snappy.parquet  ← Bucket 1
├── ...
└── part-00000-xxx_00009.c000.snappy.parquet  ← Bucket 9
"""

# Read bucketed table
print("\nReading bucketed table:")
df_bucketed = spark.table("transactions_bucketed")
print(f"  Records: {df_bucketed.count()}")

# ============================================================================
# TASK 2: Join Optimization with Bucketing
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: BUCKETED JOIN OPTIMIZATION")
print("=" * 80)

"""
Scenario: Join two large tables on same column
- Both bucketed by join key
- Same number of buckets
- No shuffle needed!
"""

# Create second bucketed table (customer info)
print("\nCreating second bucketed table...")

import random
customer_data = [(f"CUST{i:04d}", f"Customer_{i}", 
                  random.choice(["Premium", "Regular", "Basic"]))
                 for i in range(1, 1001)]

df_customers = spark.createDataFrame(
    customer_data, 
    ["customer_id", "customer_name", "tier"]
)

# Bucket by same column with same number of buckets
spark.sql("DROP TABLE IF EXISTS customers_bucketed")

df_customers.write \
  .mode("overwrite") \
  .bucketBy(10, "customer_id") \
  .saveAsTable("customers_bucketed")

print("✓ Second bucketed table created")

# Join bucketed tables
print("\nJoining bucketed tables...")

df_trans_bucketed = spark.table("transactions_bucketed")
df_cust_bucketed = spark.table("customers_bucketed")

joined = df_trans_bucketed.join(df_cust_bucketed, "customer_id")

print(f"  Joined records: {joined.count()}")

print("\nExecution plan (should show no Exchange/Shuffle):")
joined.explain()

"""
In the plan, look for:
✓ No "Exchange" operator (no shuffle!)
✓ "BucketedHashJoin" or similar
✓ Much faster than regular join

Requirements for bucketed join optimization:
1. Same bucket column
2. Same number of buckets
3. Both tables bucketed
4. spark.sql.sources.bucketing.enabled = true (default)
"""

# ============================================================================
# TASK 3: Bucket + Partition Combination
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: COMBINING PARTITIONING AND BUCKETING")
print("=" * 80)

"""
Advanced: Use both partitioning AND bucketing
- Partition by date (time-based filtering)
- Bucket by user_id (join optimization)
- Best of both worlds!
"""

print("\nCreating partitioned + bucketed table...")

df_with_date = df.withColumn("year", year(to_date("transaction_date"))) \
                 .withColumn("month", month(to_date("transaction_date")))

spark.sql("DROP TABLE IF EXISTS transactions_partitioned_bucketed")

partition_bucket_path = "output/partitioned_bucketed"

df_with_date.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .bucketBy(10, "customer_id") \
  .sortBy("transaction_date") \
  .saveAsTable(
    "transactions_partitioned_bucketed",
    format="parquet",
    path=partition_bucket_path
  )

print("✓ Partitioned + Bucketed table created")

"""
Directory Structure:

output/partitioned_bucketed/
├── year=2024/
│   ├── month=1/
│   │   ├── part-xxx_00000.c000.parquet  ← Bucket 0
│   │   ├── part-xxx_00001.c000.parquet  ← Bucket 1
│   │   ├── ...
│   │   └── part-xxx_00009.c000.parquet  ← Bucket 9
│   └── month=2/
│       ├── part-xxx_00000.c000.parquet
│       └── ...
└── year=2025/
    └── ...

Benefits:
✓ Partition pruning by date
✓ Bucketed join optimization
✓ Sorted within each bucket-partition
✓ Optimal for time-series with joins
"""

# Query with both optimizations
df_combined = spark.table("transactions_partitioned_bucketed")

query = df_combined.filter(
    (col("year") == 2024) & (col("month") == 1)
).join(
    df_cust_bucketed, "customer_id"
)

print(f"\nQuery result: {query.count()} records")
print("\nPlan (partition pruning + bucketed join):")
query.explain()

# ============================================================================
# TASK 4: MaxRecordsPerFile
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: CONTROLLING FILE SIZE WITH maxRecordsPerFile")
print("=" * 80)

"""
Control output file size:
- Limit records per file
- Prevent too-large files
- Better for systems with size limits
"""

print("\nWriting with maxRecordsPerFile...")

max_records_path = "output/max_records_per_file"

MAX_RECORDS = 1000  # Small for demonstration

df.write \
  .mode("overwrite") \
  .option("maxRecordsPerFile", MAX_RECORDS) \
  .parquet(max_records_path)

print(f"✓ Written with max {MAX_RECORDS} records per file")

# Count output files
import os
try:
    files = [f for f in os.listdir(max_records_path) if f.endswith('.parquet')]
    print(f"  Output files: {len(files)}")
    print(f"  Expected files: ~{df.count() // MAX_RECORDS}")
except:
    print("  (File count not available)")

"""
Use Cases:
✓ Hive/Impala small file optimization
✓ S3 multipart upload size limits
✓ Parquet reader memory constraints
✓ Even file size distribution

Configuration:
.option("maxRecordsPerFile", 10000)
.option("maxFileSize", "128MB")  # Spark 3.0+
"""

# ============================================================================
# TASK 5: Managed vs External Tables
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: MANAGED vs EXTERNAL TABLES")
print("=" * 80)

"""
Managed Table:
- Spark controls both metadata and data
- saveAsTable() without path
- DROP TABLE deletes data

External Table:
- Spark controls metadata only
- saveAsTable() with path
- DROP TABLE keeps data
"""

print("\n1. Managed Table:")
df.limit(100).write \
  .mode("overwrite") \
  .saveAsTable("managed_table")

print("   ✓ Created managed table")
print("   Location: spark-warehouse/managed_table")

# Check table info
managed_info = spark.sql("DESCRIBE EXTENDED managed_table")
print("\n   Table type:")
managed_info.filter(col("col_name") == "Type").show(truncate=False)

print("\n2. External Table:")
external_path = "output/external_table"

df.limit(100).write \
  .mode("overwrite") \
  .option("path", external_path) \
  .saveAsTable("external_table")

print(f"   ✓ Created external table")
print(f"   Location: {external_path}")

# Check table info
external_info = spark.sql("DESCRIBE EXTENDED external_table")
print("\n   Table type:")
external_info.filter(col("col_name") == "Type").show(truncate=False)

"""
Key Differences:

Managed Table:
✓ Simpler (no path needed)
✓ Spark manages everything
✗ DROP TABLE deletes data!
Use: Temporary analysis, Spark-owned data

External Table:
✓ Data survives DROP TABLE
✓ Share data with other tools
✓ Explicit location
Use: Production data, shared data lakes

Recommendation: Use external tables in production
"""

# Cleanup
spark.sql("DROP TABLE IF EXISTS managed_table")
spark.sql("DROP TABLE IF EXISTS external_table")

# ============================================================================
# TASK 6: Write Optimizations Summary
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: WRITE OPTIMIZATION STRATEGIES")
print("=" * 80)

strategies = """
┌────────────────────┬───────────────────────┬──────────────────────┐
│ Optimization       │ Use Case              │ Command              │
├────────────────────┼───────────────────────┼──────────────────────┤
│ Partitioning       │ Date range queries    │ .partitionBy("date") │
│ Bucketing          │ Frequent joins        │ .bucketBy(N, "id")   │
│ Sorting            │ Range queries         │ .sortBy("col")       │
│ Both               │ Complex queries       │ Combine both         │
│ MaxRecords         │ File size control     │ .option("maxRecords")│
│ Compression        │ Storage efficiency    │ .option("compression")│
│ Coalesce           │ Reduce file count     │ .coalesce(N)         │
└────────────────────┴───────────────────────┴──────────────────────┘

Decision Tree:

1. Time-series data?
   → YES: partitionBy("year", "month", "day")
   → NO: Continue

2. Frequent joins on same column?
   → YES: bucketBy(N, "join_column")
   → NO: Continue

3. Range queries?
   → YES: sortBy("range_column")
   → NO: Continue

4. Too many small files?
   → YES: coalesce(N) or repartition(N)
   → NO: Done

5. Need even file sizes?
   → YES: .option("maxRecordsPerFile", N)
   → NO: Done
"""

print(strategies)

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. BUCKETING:
   ✓ Pre-shuffle data into buckets
   ✓ Optimizes joins on bucket column
   ✓ Requires same # buckets for join optimization
   ✓ Only works with saveAsTable
   ✓ Bucket count: 10-200 typically

2. BUCKETING + PARTITIONING:
   ✓ Partition for date filtering
   ✓ Bucket for join optimization
   ✓ Best of both worlds
   ✓ Order: partitionBy then bucketBy

3. SORTING:
   ✓ sortBy for ordering within buckets
   ✓ Faster range queries
   ✓ Better compression
   ✓ Combined with bucketing

4. FILE SIZE CONTROL:
   ✓ maxRecordsPerFile: Limit records
   ✓ maxFileSize: Limit bytes (Spark 3.0+)
   ✓ Prevents too-large files
   ✓ Better for downstream systems

5. TABLE TYPES:
   ✓ Managed: Spark controls data & metadata
   ✓ External: Data independent of metadata
   ✓ Production: Use external tables
   ✓ DROP external preserves data

6. OPTIMIZATION CHECKLIST:
   □ Partition by frequently filtered columns
   □ Bucket by frequently joined columns
   □ Sort for range queries
   □ Control file sizes
   □ Use compression
   □ Test query performance

7. BEST PRACTICES:
   ✓ External tables for production
   ✓ Document bucketing strategy
   ✓ Test join performance
   ✓ Monitor file sizes
   ✓ Verify optimizations with explain()

8. COMMON PATTERNS:
   Time-series + joins:
     .partitionBy("date")
     .bucketBy(50, "user_id")
     .sortBy("timestamp")
   
   Dimension table:
     .bucketBy(10, "id")
   
   Fact table:
     .partitionBy("date")
     .bucketBy(100, "user_id")
"""

print(takeaways)

# Cleanup
spark.sql("DROP TABLE IF EXISTS transactions_bucketed")
spark.sql("DROP TABLE IF EXISTS customers_bucketed")
spark.sql("DROP TABLE IF EXISTS transactions_partitioned_bucketed")

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Bucketing + Partitioning = Ultimate optimization!")
