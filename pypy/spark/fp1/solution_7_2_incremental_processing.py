"""
Problem 7.2: Incremental Processing
====================================
Goal: Implement incremental data processing pattern

Key Concepts:
- Watermarking for new data identification
- Merge operations (upserts)
- State management
- Change Data Capture (CDC)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("Problem 7.2 - Incremental Processing") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 7.2: INCREMENTAL PROCESSING")
print("=" * 80)

# ============================================================================
# SETUP: Create Existing and New Data
# ============================================================================

print("\n" + "-" * 80)
print("SETUP: Creating Existing and New Data")
print("-" * 80)

# Existing processed data (Day 1-5)
df_existing = spark.read.parquet("transactions.parquet") \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .withColumn("processed_timestamp", lit(datetime.now() - timedelta(days=1)))

# Simulate: Process only recent data
cutoff_date = datetime.now() - timedelta(days=10)
df_existing = df_existing.filter(col("transaction_date") >= lit(cutoff_date.date()))

existing_path = "output/processed_data"
df_existing.write.mode("overwrite").partitionBy("transaction_date").parquet(existing_path)

print(f"Existing processed data: {df_existing.count()} records")

# New data (Day 6 with some updates)
df_new = spark.read.parquet("transactions.parquet") \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .withColumn("processed_timestamp", current_timestamp())

# Simulate new data: Last 3 days
new_cutoff = datetime.now() - timedelta(days=3)
df_new = df_new.filter(col("transaction_date") >= lit(new_cutoff.date()))

print(f"New incoming data: {df_new.count()} records")

# ============================================================================
# TASK 1: Identify New Records (Watermarking)
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: IDENTIFY NEW RECORDS")
print("=" * 80)

"""
Watermarking: Track what's been processed

Methods:
1. High Water Mark (HWM): Track max timestamp/id
2. Delta Lake: Transaction log
3. Custom tracking table
"""

# Method 1: High Water Mark
print("\n1. High Water Mark Approach:")

# Get last processed timestamp
last_processed = df_existing.agg(max("processed_timestamp")).collect()[0][0]
print(f"   Last processed: {last_processed}")

# Filter new data
df_new_only = df_new.filter(col("processed_timestamp") > lit(last_processed))
print(f"   New records: {df_new_only.count()}")

# Method 2: Anti-Join
print("\n2. Anti-Join Approach (find missing):")

df_missing = df_new.join(
    df_existing.select("transaction_id"),
    "transaction_id",
    "left_anti"
)

print(f"   Missing records: {df_missing.count()}")

# Method 3: Date-based
print("\n3. Date-based Approach:")

# Get latest date in existing data
latest_date = df_existing.agg(max("transaction_date")).collect()[0][0]
print(f"   Latest date processed: {latest_date}")

# Get new dates
df_new_dates = df_new.filter(col("transaction_date") > lit(latest_date))
print(f"   New date records: {df_new_dates.count()}")

# ============================================================================
# TASK 2: Merge Operations (Upsert)
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: MERGE/UPSERT OPERATIONS")
print("=" * 80)

"""
Upsert: Update existing + Insert new

SQL MERGE equivalent in Spark:
1. Identify updates and inserts
2. Update existing records
3. Insert new records
4. Union both
"""

# Simulate updates: Some existing records with changed amounts
df_updates = df_existing.sample(0.1).withColumn(
    "amount",
    col("amount") * 1.1
).withColumn(
    "processed_timestamp",
    current_timestamp()
)

print(f"\nRecords to update: {df_updates.count()}")
print(f"Records to insert: {df_missing.count()}")

# UPSERT Implementation
print("\nPerforming UPSERT...")

# Step 1: Identify keys to update
update_keys = df_updates.select("transaction_id")

# Step 2: Remove old versions of updated records
df_without_updates = df_existing.join(
    update_keys,
    "transaction_id",
    "left_anti"
)

# Step 3: Union: (existing - updates) + updates + inserts
df_merged = df_without_updates \
    .union(df_updates) \
    .union(df_missing)

print(f"\nMerged data:")
print(f"  Before: {df_existing.count()}")
print(f"  After:  {df_merged.count()}")
print(f"  Added:  {df_merged.count() - df_existing.count()}")

# ============================================================================
# TASK 3: Write Back with Partitioning
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: INCREMENTAL WRITE")
print("=" * 80)

"""
Strategies:
1. Overwrite all (simple, inefficient)
2. Dynamic partition overwrite (efficient)
3. Append new partitions only
"""

# Strategy 1: Full overwrite (not recommended)
print("\n1. Full Overwrite (NOT recommended for incremental):")
print("   df.write.mode('overwrite').parquet(path)")
print("   ✗ Rewrites all data (slow)")
print("   ✗ No history preservation")

# Strategy 2: Dynamic partition overwrite (recommended)
print("\n2. Dynamic Partition Overwrite (RECOMMENDED):")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

output_incremental = "output/incremental_data"

df_merged.write \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .parquet(output_incremental)

print(f"   ✓ Written to: {output_incremental}")
print("   ✓ Only affected partitions overwritten")
print("   ✓ Other partitions untouched")

# Reset
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

# Strategy 3: Append mode
print("\n3. Append Mode (for new partitions only):")
print("   df_new.write.mode('append').partitionBy('date').parquet(path)")
print("   ✓ Fast for new data")
print("   ⚠️  Creates duplicates if run twice")
print("   ⚠️  No updates, only inserts")

# ============================================================================
# TASK 4: CDC Pattern
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: CHANGE DATA CAPTURE (CDC)")
print("=" * 80)

"""
CDC: Track all changes to data

Operations:
- INSERT: New record
- UPDATE: Modified record
- DELETE: Removed record

Implementation:
- Add operation_type column
- Add change_timestamp
- Maintain full history
"""

# Simulate CDC feed
print("\nProcessing CDC feed...")

# Existing records
existing_ids = df_existing.select("transaction_id").distinct()

# Classify operations
df_cdc = df_new.join(
    existing_ids.withColumn("exists", lit(True)),
    "transaction_id",
    "left"
).withColumn(
    "operation_type",
    when(col("exists").isNull(), "INSERT")
    .otherwise("UPDATE")
).withColumn(
    "change_timestamp",
    current_timestamp()
).drop("exists")

# Show CDC summary
print("\nCDC Summary:")
df_cdc.groupBy("operation_type").count().show()

# CDC table (append-only)
cdc_path = "output/cdc_log"

df_cdc.write \
    .mode("append") \
    .partitionBy("transaction_date") \
    .parquet(cdc_path)

print(f"✓ CDC log written to: {cdc_path}")

# ============================================================================
# TASK 5: Incremental Aggregations
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: INCREMENTAL AGGREGATIONS")
print("=" * 80)

"""
Challenge: Update aggregations incrementally

Naive: Recompute all
Better: Update only affected aggregations
"""

# Existing aggregations
agg_existing = df_existing.groupBy("customer_id").agg(
    count("*").alias("transaction_count"),
    sum("amount").alias("total_spent")
)

print(f"\nExisting aggregations: {agg_existing.count()} customers")

# New data aggregations
agg_new = df_new.groupBy("customer_id").agg(
    count("*").alias("new_transactions"),
    sum("amount").alias("new_spending")
)

print(f"New data aggregations: {agg_new.count()} customers")

# Merge aggregations
agg_merged = agg_existing.join(agg_new, "customer_id", "outer") \
    .withColumn(
        "transaction_count",
        coalesce(col("transaction_count"), lit(0)) + coalesce(col("new_transactions"), lit(0))
    ).withColumn(
        "total_spent",
        coalesce(col("total_spent"), lit(0)) + coalesce(col("new_spending"), lit(0))
    ).select("customer_id", "transaction_count", "total_spent")

print(f"\nMerged aggregations: {agg_merged.count()} customers")
agg_merged.show(10)

# ============================================================================
# TASK 6: Idempotent Processing
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: IDEMPOTENT PROCESSING")
print("=" * 80)

"""
Idempotent: Running twice produces same result

Techniques:
1. Unique keys for deduplication
2. Timestamp-based filtering
3. Transaction IDs
4. Checkpointing
"""

print("\nIdempotent Pattern:")

# Add processing metadata
df_idempotent = df_new.withColumn(
    "processing_id",
    lit("batch_20241202_01")  # Unique batch ID
).withColumn(
    "processing_timestamp",
    current_timestamp()
)

# Deduplication before write
df_deduped = df_idempotent.dropDuplicates(["transaction_id", "processing_id"])

print(f"  Before dedup: {df_idempotent.count()}")
print(f"  After dedup: {df_deduped.count()}")
print("  ✓ Safe to run multiple times")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. WATERMARKING:
   ✓ Track max timestamp/ID
   ✓ Filter new data efficiently
   ✓ Anti-join for missing records
   ✓ Date-based partitioning helps

2. MERGE/UPSERT:
   ✓ Identify updates vs inserts
   ✓ Remove old versions (anti-join)
   ✓ Union: (existing - updates) + updates + inserts
   ✓ Use Delta Lake for native MERGE

3. INCREMENTAL WRITES:
   ✓ Dynamic partition overwrite (best)
   ✓ Append for new partitions only
   ✗ Avoid full table overwrite

4. CDC PATTERN:
   ✓ Track INSERT, UPDATE, DELETE
   ✓ Append-only log
   ✓ Full history preservation
   ✓ Change timestamps

5. INCREMENTAL AGGREGATIONS:
   ✓ Outer join existing + new
   ✓ Coalesce nulls to 0
   ✓ Add incremental values
   ✓ Recompute only affected

6. IDEMPOTENCY:
   ✓ Use unique batch IDs
   ✓ Deduplication
   ✓ Timestamp-based filtering
   ✓ Safe to rerun

7. BEST PRACTICES:
   ✓ Partition by date
   ✓ Track processing metadata
   ✓ Use dynamic partition overwrite
   ✓ Implement checkpointing
   ✓ Monitor for duplicates
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
