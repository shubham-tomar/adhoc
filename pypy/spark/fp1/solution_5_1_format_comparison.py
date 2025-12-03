"""
Problem 5.1: Format Comparison
===============================
Goal: Write data in different formats and compare characteristics

Key Concepts:
- Output formats: CSV, JSON, Parquet, ORC
- Compression options
- Save modes
- Performance and file size comparison
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import os

spark = SparkSession.builder \
    .appName("Problem 5.1 - Format Comparison") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 5.1: FORMAT COMPARISON")
print("=" * 80)

# Prepare aggregated data for writing
df = spark.read.parquet("transactions.parquet")

result_df = df.filter(col("amount") > 100).groupBy("category").agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count"),
    avg("amount").alias("avg_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
)

print("\nData to write:")
result_df.show()
print(f"Records: {result_df.count()}")

# ============================================================================
# PART 1: CSV Format
# ============================================================================

print("\n" + "=" * 80)
print("PART 1: CSV FORMAT")
print("=" * 80)

"""
CSV (Comma-Separated Values):
- Human-readable text format
- Wide compatibility
- Schema not included (must infer or specify)
- Slower than binary formats
"""

print("\nWriting as CSV with header...")

csv_path = "output/format_csv"

# Basic CSV write
start = time.time()
result_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(csv_path)
time_csv = time.time() - start

print(f"  Time: {time_csv:.4f}s")
print(f"  Path: {csv_path}")

"""
CSV Options:

header: true/false
- Include column names in first row

delimiter: "," (default), "|", "\t"
- Field separator

quote: '"' (default)
- Character to quote fields containing delimiter

escape: "\\" (default)
- Escape character

nullValue: null representation
- Default: empty string

dateFormat: "yyyy-MM-dd"
compression: "gzip", "snappy", "lzo"
"""

# CSV with custom options
csv_custom_path = "output/format_csv_custom"

result_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .option("quote", "'") \
    .option("nullValue", "NULL") \
    .option("compression", "gzip") \
    .csv(csv_custom_path)

print(f"  Custom CSV written to: {csv_custom_path}")

# Read back and verify
print("\nReading CSV back:")
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)

df_csv.printSchema()
df_csv.show()

# ============================================================================
# PART 2: JSON Format
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: JSON FORMAT")
print("=" * 80)

"""
JSON (JavaScript Object Notation):
- Semi-structured format
- Supports nested data
- Self-describing (schema embedded)
- Slower than binary formats
- Larger file size
"""

print("\nWriting as JSON...")

json_path = "output/format_json"

start = time.time()
result_df.coalesce(1).write \
    .mode("overwrite") \
    .json(json_path)
time_json = time.time() - start

print(f"  Time: {time_json:.4f}s")
print(f"  Path: {json_path}")

"""
JSON Options:

compression: "gzip", "bzip2", "lz4", "snappy"
dateFormat: "yyyy-MM-dd"
timestampFormat: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
"""

# JSON with compression
json_compressed_path = "output/format_json_compressed"

result_df.coalesce(1).write \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .json(json_compressed_path)

print(f"  Compressed JSON: {json_compressed_path}")

# Read back
print("\nReading JSON back:")
df_json = spark.read.json(json_path)
df_json.printSchema()
df_json.show()

# ============================================================================
# PART 3: Parquet Format
# ============================================================================

print("\n" + "=" * 80)
print("PART 3: PARQUET FORMAT")
print("=" * 80)

"""
Parquet:
- Columnar binary format
- Optimized for analytics
- Built-in compression
- Schema in metadata
- Supports predicate pushdown
- BEST for Spark/Hadoop ecosystem
"""

print("\nWriting as Parquet...")

parquet_path = "output/format_parquet"

start = time.time()
result_df.coalesce(1).write \
    .mode("overwrite") \
    .parquet(parquet_path)
time_parquet = time.time() - start

print(f"  Time: {time_parquet:.4f}s")
print(f"  Path: {parquet_path}")

"""
Parquet Compression Options:

snappy (default):
✓ Fast compression/decompression
✓ Moderate compression ratio (~2:1)
✓ Best for general use

gzip:
✓ Better compression ratio (~4:1)
✗ Slower decompression
✓ Good for cold storage

lz4:
✓ Fastest compression
✓ Lower compression ratio
✓ Good for hot data

zstd:
✓ Best compression ratio
✓ Fast decompression
✓ Good balance (Spark 2.4+)

uncompressed:
✗ Largest files
✓ Fastest read
✗ Rarely used
"""

# Test different compressions
compressions = ["snappy", "gzip", "lz4", "uncompressed"]
compression_results = []

for codec in compressions:
    path = f"output/format_parquet_{codec}"
    
    start = time.time()
    result_df.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", codec) \
        .parquet(path)
    write_time = time.time() - start
    
    # Get file size
    total_size = 0
    try:
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.parquet'):
                    total_size += os.path.getsize(os.path.join(root, file))
    except:
        total_size = 0
    
    compression_results.append((codec, write_time, total_size))
    print(f"  {codec:<15}: {write_time:.4f}s, {total_size/1024:.1f}KB")

# Read back
print("\nReading Parquet back:")
df_parquet = spark.read.parquet(parquet_path)
df_parquet.printSchema()
df_parquet.show()

# ============================================================================
# PART 4: ORC Format
# ============================================================================

print("\n" + "=" * 80)
print("PART 4: ORC FORMAT")
print("=" * 80)

"""
ORC (Optimized Row Columnar):
- Similar to Parquet (columnar)
- Optimized for Hive
- Built-in compression
- Good for Hadoop ecosystem
- Slightly better compression than Parquet
"""

print("\nWriting as ORC...")

orc_path = "output/format_orc"

start = time.time()
result_df.coalesce(1).write \
    .mode("overwrite") \
    .orc(orc_path)
time_orc = time.time() - start

print(f"  Time: {time_orc:.4f}s")
print(f"  Path: {orc_path}")

"""
ORC Compression Options:

snappy (default)
zlib
lzo
zstd
none
"""

# Read back
print("\nReading ORC back:")
df_orc = spark.read.orc(orc_path)
df_orc.printSchema()
df_orc.show()

# ============================================================================
# PART 5: Save Modes
# ============================================================================

print("\n" + "=" * 80)
print("PART 5: SAVE MODES")
print("=" * 80)

"""
Save Modes control behavior when data already exists:

1. overwrite: Delete existing data and write new
2. append: Add to existing data
3. ignore: Skip if data exists
4. error (default): Fail if data exists
"""

test_path = "output/save_mode_test"

# Mode 1: ERROR (default)
print("\n1. ERROR mode (default):")
result_df.write.mode("error").parquet(test_path)
print("   ✓ Initial write succeeded")

try:
    result_df.write.mode("error").parquet(test_path)
    print("   ✓ Second write succeeded")
except Exception as e:
    print(f"   ✗ Second write failed (expected): {type(e).__name__}")

# Mode 2: OVERWRITE
print("\n2. OVERWRITE mode:")
result_df.filter(col("total_amount") > 5000).write \
    .mode("overwrite") \
    .parquet(test_path)
count_after_overwrite = spark.read.parquet(test_path).count()
print(f"   ✓ Overwrote data")
print(f"   Records after overwrite: {count_after_overwrite}")

# Mode 3: APPEND
print("\n3. APPEND mode:")
initial_count = spark.read.parquet(test_path).count()
result_df.filter(col("total_amount") <= 5000).write \
    .mode("append") \
    .parquet(test_path)
final_count = spark.read.parquet(test_path).count()
print(f"   ✓ Appended data")
print(f"   Before: {initial_count}, After: {final_count}")

# Mode 4: IGNORE
print("\n4. IGNORE mode:")
result_df.write.mode("ignore").parquet(test_path)
print("   ✓ Silently skipped (data already exists)")

"""
When to Use Each Mode:

ERROR (default):
✓ Safety first
✓ Prevent accidental overwrites
✓ Production pipelines

OVERWRITE:
✓ Replace existing data
✓ Reprocessing
✓ Daily refreshes
⚠️  Data loss risk!

APPEND:
✓ Incremental loads
✓ Streaming writes
✓ Log files
⚠️  Can create duplicates!

IGNORE:
✓ Idempotent writes
✓ Skip if already processed
✓ Backfill scenarios
"""

# ============================================================================
# PART 6: Comparison Summary
# ============================================================================

print("\n" + "=" * 80)
print("FORMAT COMPARISON SUMMARY")
print("=" * 80)

def get_dir_size(path):
    """Get total size of directory"""
    total = 0
    try:
        for root, dirs, files in os.walk(path):
            for file in files:
                if not file.startswith('.'):
                    total += os.path.getsize(os.path.join(root, file))
    except:
        pass
    return total

# Collect metrics
formats = [
    ("CSV", csv_path, time_csv),
    ("JSON", json_path, time_json),
    ("Parquet", parquet_path, time_parquet),
    ("ORC", orc_path, time_orc)
]

print(f"\n{'Format':<15} {'Write Time':<15} {'File Size':<15} {'Compression':<15}")
print("-" * 60)

for name, path, write_time in formats:
    size = get_dir_size(path)
    size_kb = size / 1024
    print(f"{name:<15} {write_time:<15.4f} {size_kb:<15.1f}KB", end="")
    
    if size > 0:
        # Calculate compression ratio vs CSV
        csv_size = get_dir_size(csv_path)
        if csv_size > 0:
            ratio = csv_size / size
            print(f"{ratio:<15.2f}x")
        else:
            print()
    else:
        print()

# Read performance comparison
print("\n" + "-" * 60)
print("READ PERFORMANCE TEST")
print("-" * 60)

for name, path, _ in formats:
    start = time.time()
    df_read = spark.read.format(name.lower()).load(path)
    count = df_read.count()
    read_time = time.time() - start
    print(f"{name:<15} read time: {read_time:.4f}s")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. FORMAT SELECTION:

   CSV:
   ✓ Human-readable
   ✓ Wide compatibility
   ✗ Slow, large files
   ✗ No schema
   Use: Data exchange, Excel

   JSON:
   ✓ Semi-structured
   ✓ Nested data support
   ✗ Slower than binary
   ✗ Verbose (large files)
   Use: APIs, logs, nested data

   Parquet:
   ✓ FASTEST for analytics
   ✓ Best compression
   ✓ Column pruning
   ✓ Predicate pushdown
   Use: Data lakes, Spark processing

   ORC:
   ✓ Similar to Parquet
   ✓ Optimized for Hive
   ✓ Slightly better compression
   Use: Hive/Hadoop ecosystems

2. COMPRESSION:
   snappy: Fast, moderate compression (default)
   gzip: Better compression, slower
   lz4: Fastest, lower compression
   zstd: Best balance (recommended)

3. SAVE MODES:
   error: Safe, fails if exists (default)
   overwrite: Replace all data
   append: Add to existing
   ignore: Skip if exists

4. BEST PRACTICES:
   ✓ Use Parquet for Spark analytics
   ✓ Use snappy or zstd compression
   ✓ coalesce before writing (control files)
   ✓ Partition large datasets
   ✓ Test read performance

5. ANTI-PATTERNS:
   ✗ CSV for large analytics workloads
   ✗ JSON for structured tabular data
   ✗ No compression (waste space)
   ✗ Too many small files
   ✗ Wrong save mode (data loss)

6. PERFORMANCE RANKING (Spark):
   1. Parquet: ⭐⭐⭐⭐⭐
   2. ORC: ⭐⭐⭐⭐
   3. JSON: ⭐⭐
   4. CSV: ⭐

7. FILE SIZE RANKING (Compressed):
   1. Parquet: Smallest
   2. ORC: Very small
   3. JSON (gzip): Medium
   4. CSV: Largest

8. WHEN TO USE WHAT:
   Analytics: Parquet + zstd
   Data Exchange: CSV
   APIs/Logs: JSON
   Hive: ORC
   Archive: Parquet + gzip
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 For Spark: Parquet is almost always the right choice!")
