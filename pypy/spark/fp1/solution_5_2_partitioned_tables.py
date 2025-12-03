"""
Problem 5.2: Partitioned Tables
================================
Goal: Write partitioned tables and understand partition pruning

Key Concepts:
- Partitioned writes
- Partition column handling
- Partition pruning verification
- Hive-style partitioning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Problem 5.2 - Partitioned Tables") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 5.2: PARTITIONED TABLES")
print("=" * 80)

# Prepare data
df = spark.read.parquet("transactions.parquet")
df = df.withColumn("transaction_date", to_date(col("transaction_date"))) \
       .withColumn("year", year("transaction_date")) \
       .withColumn("month", month("transaction_date")) \
       .withColumn("day", dayofmonth("transaction_date"))

print(f"\nDataset: {df.count()} records")

# ============================================================================
# TASK 1: Year/Month/Day Hierarchy
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: HIERARCHICAL PARTITIONING (YEAR/MONTH/DAY)")
print("=" * 80)

"""
Hierarchical Partitioning:
- Organize data in nested directory structure
- Enables efficient date range queries
- Standard pattern for time-series data
"""

print("\nWriting with year/month/day hierarchy...")

output_path_1 = "output/partitioned_date_hierarchy"

df.write \
  .mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .parquet(output_path_1)

print(f"✓ Written to: {output_path_1}")

"""
Directory Structure:

output/partitioned_date_hierarchy/
├── year=2024/
│   ├── month=1/
│   │   ├── day=1/
│   │   │   └── part-xxx.parquet
│   │   ├── day=2/
│   │   │   └── part-xxx.parquet
│   │   └── ...
│   ├── month=2/
│   │   └── ...
└── year=2025/
    └── ...

Benefits:
✓ Efficient date range queries
✓ Easy to delete old data
✓ Clear data organization
"""

# Verify partitioning
print("\nReading back with partition discovery:")
df_read = spark.read.parquet(output_path_1)

print("\nSchema (partition columns at end):")
df_read.printSchema()

print("\nSample data:")
df_read.show(5)

# ============================================================================
# TASK 2: Partition Pruning Verification
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: PARTITION PRUNING VERIFICATION")
print("=" * 80)

"""
Partition Pruning:
- Spark skips reading irrelevant partitions
- Dramatically improves query performance
- Verify with explain() plan
"""

print("\nTest 1: Query specific month")
query1 = df_read.filter((col("year") == 2024) & (col("month") == 1))

print(f"  Records: {query1.count()}")
print("\n  Physical Plan (look for 'PartitionFilters'):")
query1.explain()

"""
In the plan, look for:
- PartitionFilters: [isnotnull(year#123), isnotnull(month#124), (year#123 = 2024), (month#124 = 1)]
- ReadSchema: Shows only non-partition columns
- PushedFilters: Partition filters pushed to file scan
"""

print("\n" + "-" * 80)
print("Test 2: Range query (January to March 2024)")
query2 = df_read.filter(
    (col("year") == 2024) &
    (col("month") >= 1) &
    (col("month") <= 3)
)

print(f"  Records: {query2.count()}")
print("\n  Physical Plan:")
query2.explain()

print("\n" + "-" * 80)
print("Test 3: Query without partition columns (FULL SCAN)")
query3 = df_read.filter(col("amount") > 500)

print(f"  Records: {query3.count()}")
print("\n  Physical Plan (NO partition pruning):")
query3.explain()

"""
Partition Pruning Rules:

✓ PRUNED:
- WHERE year = 2024
- WHERE year = 2024 AND month = 1
- WHERE year IN (2023, 2024)
- WHERE year = 2024 AND month BETWEEN 1 AND 3

✗ NOT PRUNED:
- WHERE month = 1 (year not filtered)
- WHERE amount > 100 (non-partition column)
- WHERE YEAR(transaction_date) = 2024 (function on column)

Key Rule: Can only prune on PREFIX of partition columns
partitionBy("year", "month", "day") → Must filter year to prune month
"""

# ============================================================================
# TASK 3: Partition by Category
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: PARTITION BY CATEGORY")
print("=" * 80)

"""
Non-date partitioning:
- Partition by business dimension
- Good for category, region, country, etc.
- Enables parallel processing per partition
"""

print("\nWriting partitioned by category...")

output_path_2 = "output/partitioned_category"

df.write \
  .mode("overwrite") \
  .partitionBy("category") \
  .parquet(output_path_2)

print(f"✓ Written to: {output_path_2}")

"""
Directory Structure:

output/partitioned_category/
├── category=Electronics/
│   └── part-xxx.parquet
├── category=Clothing/
│   └── part-xxx.parquet
├── category=Books/
│   └── part-xxx.parquet
└── ...

Use Cases:
✓ Process categories independently
✓ Different retention per category
✓ Access control per category
✓ Easy to add/remove categories
"""

# Read and filter by category
df_category = spark.read.parquet(output_path_2)
electronics = df_category.filter(col("category") == "Electronics")

print(f"\nElectronics records: {electronics.count()}")
print("\nPartition pruning for category:")
electronics.explain()

# ============================================================================
# TASK 4: Partition Column in Output
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: HANDLING PARTITION COLUMNS IN OUTPUT")
print("=" * 80)

"""
Question: Should partition columns appear in data files?

Spark's behavior:
- Partition columns NOT written to data files
- Stored in directory names instead
- Reconstructed when reading

Example:
File: year=2024/month=1/day=15/part-xxx.parquet
- File contains: transaction_id, amount, category, ...
- File does NOT contain: year, month, day
- Spark adds year, month, day when reading from path
"""

print("\nDemonstration:")

# Check what's actually in the files
print("1. Columns in directory structure (partition columns):")
print("   year=2024, month=1, day=15")

print("\n2. Columns in Parquet files (data columns):")
# Read a single partition to see file contents
single_partition = spark.read.parquet(f"{output_path_1}/year=2024/month=*/day=*")
print(f"   Schema from files: {single_partition.columns}")

print("\n3. Columns when reading full table:")
full_table = spark.read.parquet(output_path_1)
print(f"   Schema with partitions: {full_table.columns}")

"""
Implications:

Storage:
✓ More efficient (no duplication)
✓ Smaller files
✓ Partition values not repeated

Reading:
✓ Partition columns added automatically
✓ Position: Always at the END of schema
✓ Type: Always STRING (from directory name)

Caution:
⚠️  Partition column type is STRING
⚠️  Cast if you need original type:
    df.withColumn("year", col("year").cast("int"))
"""

# Demonstrate type issue
print("\n4. Partition column types:")
full_table.select("year", "month", "day").printSchema()

# Fix by casting
print("\n5. After casting to correct types:")
fixed = full_table \
    .withColumn("year", col("year").cast("int")) \
    .withColumn("month", col("month").cast("int")) \
    .withColumn("day", col("day").cast("int"))

fixed.select("year", "month", "day").printSchema()

# ============================================================================
# TASK 5: Partition Discovery
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: PARTITION DISCOVERY")
print("=" * 80)

"""
Partition Discovery:
- Spark automatically detects partition structure
- Reads directory names to infer partitions
- Works with Hive-style partitioning (key=value)
"""

print("\nAutomatic partition discovery:")

# Spark infers partitions from directory structure
discovered = spark.read.parquet(output_path_1)

print(f"Partitions discovered: {[c for c in discovered.columns if c in ['year', 'month', 'day']]}")

# Get partition metadata
print("\nPartition information:")
partitions = discovered.select("year", "month", "day").distinct().orderBy("year", "month", "day")
print(f"  Total unique partitions: {partitions.count()}")
partitions.show(10)

"""
Partition Discovery Modes:

1. Automatic (default):
   spark.read.parquet("path")
   → Discovers partitions automatically

2. Explicit schema:
   spark.read.schema(schema).parquet("path")
   → Uses provided schema, still discovers partitions

3. Disabled:
   spark.read.option("basePath", "path").parquet("path/year=2024/*")
   → No partition discovery
"""

# ============================================================================
# TASK 6: Advanced Partition Management
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: ADVANCED PARTITION MANAGEMENT")
print("=" * 80)

"""
Techniques:
- Add new partitions
- Drop old partitions
- Compact small files
- Repartition within partitions
"""

print("\n1. Dynamic Partition Insertion (add new data):")

# Enable dynamic partition mode
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Add new data for a specific date
new_data = df.filter(
    (col("year") == 2024) &
    (col("month") == 1) &
    (col("day") == 1)
).withColumn("amount", col("amount") * 1.1)  # Modified data

new_data.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_path_1)

print("   ✓ Updated 2024-01-01 partition only")
print("   ✓ Other partitions unchanged")

# Reset to default
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

print("\n2. Drop old partitions (manual file deletion):")
print("   # Delete directories older than retention period")
print("   # Example: rm -rf output/year=2023/month=*/day=*")

print("\n3. Compact small files within partition:")
print("   df.where('year=2024 AND month=1')")
print("     .coalesce(1)")
print("     .write.mode('overwrite')")
print("     .partitionBy('year', 'month')")
print("     .parquet('output')")

# ============================================================================
# TASK 7: Partition Column Best Practices
# ============================================================================

print("\n" + "=" * 80)
print("TASK 7: PARTITION COLUMN BEST PRACTICES")
print("=" * 80)

"""
Good Partition Columns:

✓ Low cardinality:
  - Date fields (year, month, day)
  - Category, region, country
  - Status, type

✗ High cardinality:
  - User ID (millions of values)
  - Transaction ID (unique per row)
  - Timestamp (too many values)

Guidelines:
- Aim for 100-10,000 partitions total
- Partition size: 128MB - 1GB
- Balance between granularity and performance
"""

# Check partition cardinality
print("\nPartition cardinality analysis:")

partitioning_candidates = ["category", "region", "payment_method"]

for col_name in partitioning_candidates:
    distinct_count = df.select(col_name).distinct().count()
    avg_records = df.count() / distinct_count
    print(f"\n{col_name}:")
    print(f"  Distinct values: {distinct_count}")
    print(f"  Avg records per partition: {avg_records:,.0f}")
    
    if distinct_count < 10:
        print("  ✓ Good for partitioning (low cardinality)")
    elif distinct_count < 1000:
        print("  ⚠️  Consider for partitioning (medium cardinality)")
    else:
        print("  ✗ Not recommended (high cardinality)")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. PARTITIONED WRITES:
   ✓ .partitionBy("year", "month", "day")
   ✓ Creates nested directory structure
   ✓ Partition columns NOT in data files
   ✓ Reconstructed from directory names

2. PARTITION PRUNING:
   ✓ Skips reading irrelevant partitions
   ✓ Verify with explain() → PartitionFilters
   ✓ Only works on PREFIX of partition columns
   ✓ Can improve performance by 10-100x

3. GOOD PARTITION COLUMNS:
   ✓ Low cardinality (10-10,000 values)
   ✓ Date fields (year, month, day)
   ✓ Category, region, country
   ✓ Frequently filtered columns

4. BAD PARTITION COLUMNS:
   ✗ High cardinality (user_id, transaction_id)
   ✗ Unique or near-unique values
   ✗ Timestamp (use date instead)
   ✗ Rarely filtered columns

5. PARTITION MANAGEMENT:
   ✓ Dynamic overwrite for updates
   ✓ Delete old partitions for retention
   ✓ Compact small files periodically
   ✓ Monitor partition sizes

6. BEST PRACTICES:
   ✓ Partition by date for time-series
   ✓ Aim for 128MB-1GB per partition
   ✓ Total partitions: 100-10,000
   ✓ Test partition pruning
   ✓ Document partition strategy

7. ANTI-PATTERNS:
   ✗ Too many partitions (>10,000)
   ✗ Too few partitions (<10)
   ✗ Small files in many partitions
   ✗ Partitioning by unique columns
   ✗ Not verifying partition pruning

8. HIVE COMPATIBILITY:
   ✓ Use Hive-style: key=value format
   ✓ Compatible with Hive, Impala, Presto
   ✓ Standard for data lakes
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Good partitioning = Fast queries!")
