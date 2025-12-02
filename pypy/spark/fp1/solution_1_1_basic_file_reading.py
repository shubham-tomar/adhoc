"""
Problem 1.1: Basic File Reading
================================
Goal: Read transaction data from CSV, JSON, and Parquet formats
      Compare reading time and examine inferred schemas

Key Concepts:
- Different file formats have different performance characteristics
- Parquet is columnar and optimized for analytics
- JSON is flexible but slower
- CSV is human-readable but requires parsing
"""

from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Problem 1.1 - Basic File Reading") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 1.1: BASIC FILE READING")
print("=" * 80)

# ============================================================================
# SOLUTION
# ============================================================================

def time_read_operation(format_name, read_function):
    """
    Helper function to measure read time for different formats
    
    Learning Point:
    - Use time.time() for simple benchmarking
    - In production, use Spark UI metrics for detailed analysis
    """
    start_time = time.time()
    df = read_function()
    count = df.count()  # Action to trigger actual read
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"\n{format_name} Format:")
    print(f"  Read time: {elapsed:.4f} seconds")
    print(f"  Record count: {count}")
    return df, elapsed

# Read CSV
print("\n" + "-" * 80)
print("1. Reading CSV Format")
print("-" * 80)
csv_df, csv_time = time_read_operation(
    "CSV",
    lambda: spark.read.csv("transactions.csv", header=True, inferSchema=True)
)

"""
Learning Points - CSV:
- header=True: First row contains column names
- inferSchema=True: Spark scans data to infer types (adds overhead)
- CSV requires parsing each line, slower than binary formats
- Good for: Human readability, simple data exchange
"""

print("\nCSV Schema:")
csv_df.printSchema()

# Read JSON
print("\n" + "-" * 80)
print("2. Reading JSON Format")
print("-" * 80)
json_df, json_time = time_read_operation(
    "JSON",
    lambda: spark.read.json("transactions.json")
)

"""
Learning Points - JSON:
- Self-describing format (schema embedded)
- Flexible schema (can handle nested data)
- Slower than Parquet due to text parsing
- Good for: Semi-structured data, APIs, nested objects
"""

print("\nJSON Schema:")
json_df.printSchema()

# Read Parquet
print("\n" + "-" * 80)
print("3. Reading Parquet Format")
print("-" * 80)
parquet_df, parquet_time = time_read_operation(
    "Parquet",
    lambda: spark.read.parquet("transactions.parquet")
)

"""
Learning Points - Parquet:
- Columnar storage format (optimized for analytics)
- Schema stored in file metadata (no inference needed)
- Supports predicate pushdown and column pruning
- Built-in compression
- FASTEST for Spark batch processing
- Good for: Large-scale analytics, data lakes, repeated queries
"""

print("\nParquet Schema:")
parquet_df.printSchema()

# ============================================================================
# COMPARISON AND ANALYSIS
# ============================================================================

print("\n" + "=" * 80)
print("PERFORMANCE COMPARISON")
print("=" * 80)

# Create comparison summary
formats = ["CSV", "JSON", "Parquet"]
times = [csv_time, json_time, parquet_time]

print(f"\n{'Format':<15} {'Read Time (s)':<15} {'Relative Speed':<20}")
print("-" * 50)

min_time = min(times)
for fmt, t in zip(formats, times):
    relative = t / min_time
    print(f"{fmt:<15} {t:<15.4f} {relative:<20.2f}x")

"""
Expected Results:
- Parquet should be FASTEST (1.0x baseline)
- CSV should be SLOWER (2-3x slower)
- JSON should be SLOWEST (3-5x slower)

Why?
1. Parquet: Binary columnar format, no parsing needed
2. CSV: Text format, requires type inference and parsing
3. JSON: Text format + complex structure parsing
"""

# ============================================================================
# SCHEMA COMPARISON
# ============================================================================

print("\n" + "=" * 80)
print("SCHEMA COMPARISON")
print("=" * 80)

print("\nComparing data types across formats:")
print(f"{'Column':<20} {'CSV':<15} {'JSON':<15} {'Parquet':<15}")
print("-" * 65)

# Get column names from any DataFrame
columns = csv_df.columns

for col in columns[:5]:  # Show first 5 columns
    csv_type = dict(csv_df.dtypes)[col]
    json_type = dict(json_df.dtypes)[col]
    parquet_type = dict(parquet_df.dtypes)[col]
    print(f"{col:<20} {csv_type:<15} {json_type:<15} {parquet_type:<15}")

"""
Learning Points - Schema Inference:
1. CSV with inferSchema: Scans data to guess types (slow, can be wrong)
2. JSON: Self-describing, accurate types
3. Parquet: Schema in metadata, always accurate

Best Practice:
- For CSV: Define explicit schema instead of inferSchema=True
- For JSON/Parquet: Schema is reliable
"""

# ============================================================================
# SAMPLE DATA VERIFICATION
# ============================================================================

print("\n" + "=" * 80)
print("DATA VERIFICATION")
print("=" * 80)

print("\nSample records from Parquet (most reliable):")
parquet_df.show(5, truncate=False)

# Verify data consistency
print("\nVerifying data consistency across formats:")
csv_count = csv_df.count()
json_count = json_df.count()
parquet_count = parquet_df.count()

print(f"CSV records:     {csv_count}")
print(f"JSON records:    {json_count}")
print(f"Parquet records: {parquet_count}")

if csv_count == json_count == parquet_count:
    print("✅ All formats have same record count")
else:
    print("⚠️  Record count mismatch - check data integrity!")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. FILE FORMAT SELECTION:
   - Parquet: Best for Spark analytics (columnar, compressed, fast)
   - JSON: Good for semi-structured data, APIs
   - CSV: Good for human-readable data exchange

2. PERFORMANCE:
   - Parquet is 3-10x faster than CSV/JSON
   - Use Parquet for large-scale batch processing

3. SCHEMA HANDLING:
   - Parquet/JSON: Schema embedded (reliable)
   - CSV: Requires schema definition or inference (slow, error-prone)

4. WHEN TO USE EACH:
   - Parquet: Data lakes, repeated analytics, large datasets
   - JSON: API data, nested structures, log files
   - CSV: Simple data exchange, Excel compatibility, ad-hoc analysis

5. BEST PRACTICES:
   - Convert CSV/JSON to Parquet for repeated processing
   - Define explicit schemas for CSV instead of inferSchema
   - Use compression with Parquet (snappy default)
   - Partition large Parquet tables for better performance
"""

print(takeaways)

# ============================================================================
# BONUS: FILE SIZE COMPARISON
# ============================================================================

print("\n" + "=" * 80)
print("BONUS: FILE SIZE COMPARISON")
print("=" * 80)

import os

def get_file_size(filename):
    """Get file size in MB"""
    try:
        size_bytes = os.path.getsize(filename)
        size_mb = size_bytes / (1024 * 1024)
        return size_mb
    except:
        return 0

print(f"\n{'Format':<15} {'File Size (MB)':<20} {'Size Relative to Parquet':<25}")
print("-" * 60)

csv_size = get_file_size("transactions.csv")
json_size = get_file_size("transactions.json")
parquet_size = get_file_size("transactions.parquet")

if parquet_size > 0:
    print(f"{'CSV':<15} {csv_size:<20.2f} {csv_size/parquet_size:<25.2f}x")
    print(f"{'JSON':<15} {json_size:<20.2f} {json_size/parquet_size:<25.2f}x")
    print(f"{'Parquet':<15} {parquet_size:<20.2f} {'1.00x (baseline)':<25}")
else:
    print("(File size comparison not available)")

"""
Expected Results:
- Parquet is usually SMALLEST due to:
  1. Columnar compression (similar values compress well)
  2. Efficient binary encoding
  3. No redundant text/delimiters

- JSON is usually LARGEST due to:
  1. Text format (verbose)
  2. Key names repeated for each record
  
- CSV is MIDDLE size:
  1. Text format but compact
  2. No key repetition
"""

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
