"""
Problem 1.2: Schema Enforcement
================================
Goal: Define strict schema and handle malformed data with different error modes

Key Concepts:
- Schema enforcement prevents runtime errors
- Different error handling modes: PERMISSIVE, DROPMALFORMED, FAILFAST
- Data quality and validation
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Problem 1.2 - Schema Enforcement") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 1.2: SCHEMA ENFORCEMENT AND ERROR HANDLING")
print("=" * 80)

# ============================================================================
# PART 1: DEFINE STRICT SCHEMA
# ============================================================================

print("\n" + "-" * 80)
print("PART 1: DEFINING STRICT SCHEMA")
print("-" * 80)

"""
Learning Point: Why Define Schema Explicitly?

1. PERFORMANCE: No need to scan data for type inference (saves time on large files)
2. CORRECTNESS: Ensures data meets expected format
3. ERROR DETECTION: Catches bad data early
4. DOCUMENTATION: Schema serves as data contract
"""

# Define transaction schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("transaction_date", StringType(), nullable=True),  # We'll convert to date later
    StructField("category", StringType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True)
])

"""
Schema Definition Tips:
- nullable=False: Field is required (will error if missing)
- nullable=True: Field is optional
- Use appropriate types: StringType, IntegerType, DoubleType, DateType, etc.
- Consider using DecimalType for financial data (more precise than Double)
"""

print("\nDefined Schema:")
print(transaction_schema.simpleString())

# Read CSV with enforced schema
print("\nReading transactions.csv with enforced schema...")
df_with_schema = spark.read.csv(
    "transactions.csv",
    header=True,
    schema=transaction_schema  # ← Enforcing schema
)

print("\nSchema applied successfully!")
df_with_schema.printSchema()

print("\nSample data:")
df_with_schema.show(5)

# ============================================================================
# PART 2: ERROR HANDLING MODES
# ============================================================================

print("\n" + "=" * 80)
print("PART 2: ERROR HANDLING MODES")
print("=" * 80)

"""
Spark provides 3 modes for handling malformed records:

1. PERMISSIVE (default):
   - Keeps malformed records
   - Puts malformed data in _corrupt_record column
   - NULL for unparseable fields
   - SAFEST: No data loss

2. DROPMALFORMED:
   - Silently drops malformed records
   - DANGEROUS: Data loss without warning
   - Use when you trust data quality

3. FAILFAST:
   - Fails immediately on first malformed record
   - STRICTEST: Ensures data quality
   - Use for critical pipelines
"""

# First, let's see what's in our malformed file
print("\n" + "-" * 80)
print("Malformed CSV Content:")
print("-" * 80)
with open("malformed_transactions.csv", "r") as f:
    print(f.read())

"""
Issues in malformed_transactions.csv:
1. Extra column "extra_field" not in schema
2. Row 2: amount is "not_a_number" (should be numeric)
3. Row 3: amount is empty (NULL)
"""

# ============================================================================
# MODE 1: PERMISSIVE (Default)
# ============================================================================

print("\n" + "-" * 80)
print("MODE 1: PERMISSIVE (Keep Bad Records)")
print("-" * 80)

# Add _corrupt_record column to capture bad data
schema_with_corrupt = StructType(
    transaction_schema.fields + [
        StructField("_corrupt_record", StringType(), nullable=True)
    ]
)

df_permissive = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("malformed_transactions.csv", header=True, schema=schema_with_corrupt)

print("\nPERMISSIVE mode results:")
df_permissive.show(truncate=False)

print("\nAnalyzing corrupt records:")
corrupt_records = df_permissive.filter(col("_corrupt_record").isNotNull())
print(f"Found {corrupt_records.count()} corrupt records:")
corrupt_records.select("transaction_id", "customer_id", "_corrupt_record").show(truncate=False)

"""
Learning Points - PERMISSIVE:
✅ Keeps all records (no data loss)
✅ Identifies bad data via _corrupt_record
✅ Allows post-processing of bad data
❌ May pass through NULL values silently
❌ Requires explicit checking for corrupt records

Use case: Exploratory data analysis, data quality assessment
"""

# ============================================================================
# MODE 2: DROPMALFORMED
# ============================================================================

print("\n" + "-" * 80)
print("MODE 2: DROPMALFORMED (Silently Drop Bad Records)")
print("-" * 80)

df_drop = spark.read \
    .option("mode", "DROPMALFORMED") \
    .csv("malformed_transactions.csv", header=True, schema=transaction_schema)

print("\nDROPMALFORMED mode results:")
df_drop.show(truncate=False)

print(f"\nOriginal records: 3")
print(f"Records after DROPMALFORMED: {df_drop.count()}")
print(f"Records dropped: {3 - df_drop.count()}")

"""
Learning Points - DROPMALFORMED:
✅ Clean data automatically
✅ No corrupt records to handle
❌ DATA LOSS: Dropped records are gone
❌ No visibility into what was dropped
❌ Silent failures can be dangerous

Use case: Trusted data sources, non-critical pipelines
CAUTION: Track dropped records separately for audit!
"""

# ============================================================================
# MODE 3: FAILFAST
# ============================================================================

print("\n" + "-" * 80)
print("MODE 3: FAILFAST (Fail on First Bad Record)")
print("-" * 80)

print("\nAttempting to read with FAILFAST mode...")

try:
    df_failfast = spark.read \
        .option("mode", "FAILFAST") \
        .csv("malformed_transactions.csv", header=True, schema=transaction_schema)
    
    # This will trigger the read and fail
    df_failfast.show()
    
    print("✅ All records are valid!")
    
except Exception as e:
    print(f"❌ FAILFAST mode caught error:")
    print(f"   Error type: {type(e).__name__}")
    print(f"   Message: {str(e)[:200]}")  # First 200 chars

"""
Learning Points - FAILFAST:
✅ Ensures data quality (no bad data gets through)
✅ Fails early (saves processing time)
✅ Forces upstream data fixes
❌ Stops entire job on single bad record
❌ Not suitable for messy real-world data

Use case: 
- Production pipelines with strict quality requirements
- Regulated industries (finance, healthcare)
- Data contracts with SLAs
"""

# ============================================================================
# PART 3: COMPARISON AND BEST PRACTICES
# ============================================================================

print("\n" + "=" * 80)
print("ERROR MODE COMPARISON")
print("=" * 80)

comparison = """
┌────────────────┬─────────────┬───────────────┬─────────────────────┐
│ Mode           │ Data Loss   │ Job Failure   │ Bad Data Handling   │
├────────────────┼─────────────┼───────────────┼─────────────────────┤
│ PERMISSIVE     │ None        │ Never         │ _corrupt_record col │
│ DROPMALFORMED  │ Silent drop │ Never         │ Discarded           │
│ FAILFAST       │ All or none │ On first bad  │ Job stops           │
└────────────────┴─────────────┴───────────────┴─────────────────────┘

WHEN TO USE EACH:

PERMISSIVE:
✓ Exploratory data analysis
✓ Unknown data quality
✓ Need to see all records (good and bad)
✓ Building data quality reports

DROPMALFORMED:
✓ Trusted data sources
✓ Non-critical pipelines
✓ Performance-sensitive (no corrupt record tracking)
✗ Must track dropped records separately!

FAILFAST:
✓ Production pipelines
✓ Strict data quality requirements
✓ SLA-bound processing
✓ Early error detection preferred
"""

print(comparison)

# ============================================================================
# PART 4: PRODUCTION-READY ERROR HANDLING
# ============================================================================

print("\n" + "=" * 80)
print("PRODUCTION-READY ERROR HANDLING PATTERN")
print("=" * 80)

"""
Best Practice: Separate good and bad records for auditing
"""

print("\n1. Read with PERMISSIVE mode")
df_all = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("malformed_transactions.csv", header=True, schema=schema_with_corrupt)

print("\n2. Separate good and bad records")
good_records = df_all.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
bad_records = df_all.filter(col("_corrupt_record").isNotNull())

print(f"\nGood records: {good_records.count()}")
print(f"Bad records: {bad_records.count()}")

print("\n3. Process good records")
good_records.show()

print("\n4. Log/Store bad records for later analysis")
bad_records.select("_corrupt_record").show(truncate=False)

"""
Production Pattern:
1. Read with PERMISSIVE + _corrupt_record
2. Filter into good_records and bad_records
3. Process good_records in main pipeline
4. Write bad_records to error table for:
   - Audit trail
   - Data quality monitoring
   - Debugging
   - Reprocessing after fixes

Code example:
```python
# Process good data
good_records.write.parquet("output/good_data")

# Log bad data
bad_records \
    .withColumn("error_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("malformed_transactions.csv")) \
    .write.mode("append").parquet("output/error_logs")
```
"""

# ============================================================================
# PART 5: ADVANCED VALIDATION
# ============================================================================

print("\n" + "=" * 80)
print("ADVANCED VALIDATION TECHNIQUES")
print("=" * 80)

print("\nValidating data beyond schema enforcement:\n")

# Read good data
df = spark.read.csv("transactions.csv", header=True, schema=transaction_schema)

# Add validation columns
df_validated = df.withColumn(
    "is_valid",
    (col("amount") > 0) &  # Amount must be positive
    (col("quantity") > 0) &  # Quantity must be positive
    (col("customer_id").rlike("^CUST[0-9]{4}$"))  # Customer ID format check
)

print("Records with validation flag:")
df_validated.select("transaction_id", "amount", "quantity", "customer_id", "is_valid").show(5)

# Count invalid records
invalid_count = df_validated.filter(~col("is_valid")).count()
print(f"\nInvalid records found: {invalid_count}")

"""
Advanced Validation Techniques:

1. Business Rules:
   - Range checks (amount > 0, quantity > 0)
   - Pattern matching (regex for IDs)
   - Referential integrity (customer exists in customer table)

2. Statistical Validation:
   - Outlier detection
   - NULL percentage
   - Distinct value counts

3. Cross-field Validation:
   - Total = price * quantity
   - Date consistency (order_date <= ship_date)
"""

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. SCHEMA DEFINITION:
   ✓ Always define schemas explicitly (don't rely on inference)
   ✓ Use nullable=False for required fields
   ✓ Choose appropriate data types

2. ERROR MODES:
   ✓ PERMISSIVE: Default, safest, keeps all data
   ✓ DROPMALFORMED: Fast, but silent data loss
   ✓ FAILFAST: Strictest, fails on any error

3. PRODUCTION BEST PRACTICE:
   ✓ Use PERMISSIVE with _corrupt_record column
   ✓ Separate good and bad records
   ✓ Log bad records for audit/debugging
   ✓ Monitor data quality metrics

4. VALIDATION LAYERS:
   ✓ Schema enforcement (types, nullability)
   ✓ Business rules (ranges, patterns)
   ✓ Statistical checks (outliers, distributions)
   ✓ Referential integrity (joins)

5. AVOID:
   ✗ Using DROPMALFORMED without logging
   ✗ Ignoring corrupt records in PERMISSIVE mode
   ✗ Not monitoring data quality metrics
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
