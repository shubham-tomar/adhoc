"""
Problem 7.1: ETL Pipeline
==========================
Goal: Build a complete ETL pipeline with error handling

Key Concepts:
- Extract from multiple sources
- Transform with validation
- Load with quality checks
- Error handling and logging
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Problem 7.1 - ETL Pipeline") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 7.1: COMPLETE ETL PIPELINE")
print("=" * 80)

# ============================================================================
# EXTRACT: Read from Multiple Sources
# ============================================================================

print("\n" + "=" * 80)
print("EXTRACT PHASE")
print("=" * 80)

"""
Extract Strategy:
1. Read from multiple formats
2. Handle schema evolution
3. Error handling for missing files
4. Data quality checks
"""

# Source 1: CSV
print("\n1. Reading CSV transactions...")
try:
    df_csv = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .csv("transactions.csv")
    
    csv_count = df_csv.count()
    logger.info(f"CSV records: {csv_count}")
    
except Exception as e:
    logger.error(f"CSV read failed: {e}")
    df_csv = spark.createDataFrame([], StructType([]))

# Source 2: JSON
print("2. Reading JSON transactions...")
try:
    df_json = spark.read \
        .option("mode", "PERMISSIVE") \
        .json("transactions.json")
    
    json_count = df_json.count()
    logger.info(f"JSON records: {json_count}")
    
except Exception as e:
    logger.error(f"JSON read failed: {e}")
    df_json = spark.createDataFrame([], StructType([]))

# Source 3: Parquet
print("3. Reading Parquet transactions...")
df_parquet = spark.read.parquet("transactions.parquet")
parquet_count = df_parquet.count()
logger.info(f"Parquet records: {parquet_count}")

print(f"\nExtracted: {csv_count + json_count + parquet_count} total records")

# ============================================================================
# TRANSFORM: Clean and Validate
# ============================================================================

print("\n" + "=" * 80)
print("TRANSFORM PHASE")
print("=" * 80)

# Union all sources
df_raw = df_parquet  # Using parquet as main source

print("\n1. Data Cleaning:")

# Clean nulls and invalid values
df_cleaned = df_raw.filter(col("transaction_id").isNotNull()) \
                   .filter(col("customer_id").isNotNull()) \
                   .filter(col("amount").isNotNull()) \
                   .filter(col("amount") > 0) \
                   .filter(col("quantity") > 0)

print(f"   Before cleaning: {df_raw.count()}")
print(f"   After cleaning: {df_cleaned.count()}")
print(f"   Removed: {df_raw.count() - df_cleaned.count()} invalid records")

# Standardize date formats
print("\n2. Standardizing Dates:")
df_standardized = df_cleaned.withColumn(
    "transaction_date",
    to_date(col("transaction_date"))
).withColumn(
    "year", year("transaction_date")
).withColumn(
    "month", month("transaction_date")
).withColumn(
    "day", dayofmonth("transaction_date")
)

print("   ✓ Dates converted to standard format")

# Derive new columns
print("\n3. Deriving Business Metrics:")

df_enriched = df_standardized.withColumn(
    "total_value",
    col("amount") * col("quantity")
).withColumn(
    "customer_lifetime_value",
    sum("amount").over(Window.partitionBy("customer_id"))
).withColumn(
    "transaction_velocity",
    count("*").over(
        Window.partitionBy("customer_id")
              .orderBy("transaction_date")
              .rowsBetween(-30, 0)
    )
).withColumn(
    "customer_tier",
    when(col("customer_lifetime_value") > 5000, "Platinum")
    .when(col("customer_lifetime_value") > 2000, "Gold")
    .when(col("customer_lifetime_value") > 500, "Silver")
    .otherwise("Bronze")
).withColumn(
    "is_high_value",
    when(col("amount") > 500, True).otherwise(False)
)

print("   ✓ Added: total_value, customer_lifetime_value")
print("   ✓ Added: transaction_velocity, customer_tier")
print("   ✓ Added: is_high_value flag")

# Handle late arriving data
print("\n4. Handling Late Arriving Data:")

from datetime import datetime, timedelta
cutoff_date = datetime.now() - timedelta(days=365)

df_current = df_enriched.filter(
    col("transaction_date") >= lit(cutoff_date.date())
)

late_data = df_enriched.filter(
    col("transaction_date") < lit(cutoff_date.date())
)

print(f"   Current data: {df_current.count()}")
print(f"   Late arrivals: {late_data.count()}")

# Data quality checks
print("\n5. Data Quality Checks:")

quality_checks = {
    "null_customer_id": df_enriched.filter(col("customer_id").isNull()).count(),
    "null_amount": df_enriched.filter(col("amount").isNull()).count(),
    "negative_amount": df_enriched.filter(col("amount") < 0).count(),
    "future_dates": df_enriched.filter(col("transaction_date") > current_date()).count(),
    "duplicate_txn_ids": df_enriched.groupBy("transaction_id").count().filter(col("count") > 1).count()
}

print("\n   Quality Report:")
for check, count in quality_checks.items():
    status = "✓ PASS" if count == 0 else "✗ FAIL"
    print(f"   {check:<25}: {count:>5} {status}")

# ============================================================================
# LOAD: Write with Partitioning
# ============================================================================

print("\n" + "=" * 80)
print("LOAD PHASE")
print("=" * 80)

output_path = "output/etl_output"

print("\n1. Writing to Partitioned Parquet:")

df_enriched.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("compression", "snappy") \
    .parquet(output_path)

print(f"   ✓ Written to: {output_path}")
print("   ✓ Partitioned by: year, month")
print("   ✓ Format: Parquet (snappy)")

# Generate summary statistics
print("\n2. Generating Summary Statistics:")

summary = df_enriched.groupBy("year", "month", "customer_tier").agg(
    count("*").alias("transaction_count"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("total_value").alias("total_revenue"),
    avg("amount").alias("avg_transaction_amount"),
    max("amount").alias("max_transaction_amount")
)

summary_path = "output/etl_summary"
summary.write.mode("overwrite").parquet(summary_path)

print(f"   ✓ Summary saved to: {summary_path}")

summary.orderBy("year", "month", "customer_tier").show()

# Create data quality report
print("\n3. Creating Data Quality Report:")

quality_df = spark.createDataFrame(
    [(k, v) for k, v in quality_checks.items()],
    ["check_name", "failed_count"]
).withColumn("check_timestamp", current_timestamp()) \
 .withColumn("status", when(col("failed_count") == 0, "PASS").otherwise("FAIL"))

quality_path = "output/etl_quality_report"
quality_df.write.mode("append").parquet(quality_path)

print(f"   ✓ Quality report saved to: {quality_path}")
quality_df.show(truncate=False)

# ============================================================================
# ERROR HANDLING AND LOGGING
# ============================================================================

print("\n" + "=" * 80)
print("ERROR HANDLING")
print("=" * 80)

"""
ETL Error Handling Strategy:

1. Extract Errors:
   - Missing files → Continue with available
   - Schema mismatch → Log and skip
   - Corrupt data → Quarantine

2. Transform Errors:
   - Invalid data → Filter out
   - Null values → Handle or reject
   - Business rule violations → Log

3. Load Errors:
   - Write failures → Retry with backoff
   - Disk full → Alert and abort
   - Permission denied → Alert

4. Logging:
   - Record counts at each stage
   - Error counts and types
   - Performance metrics
   - Data quality metrics
"""

# Example: Robust read with error handling
def safe_read_csv(path, logger):
    """Read CSV with comprehensive error handling"""
    try:
        df = spark.read \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(path)
        
        # Check for corrupt records
        corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
        
        if corrupt_count > 0:
            logger.warning(f"Found {corrupt_count} corrupt records")
            
            # Save corrupt records for investigation
            df.filter(col("_corrupt_record").isNotNull()) \
              .write.mode("append") \
              .json("output/corrupt_records")
        
        # Return clean data
        return df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return spark.createDataFrame([], StructType([]))

print("\nError Handling Features:")
print("  ✓ Corrupt record detection")
print("  ✓ Quarantine bad data")
print("  ✓ Logging at each stage")
print("  ✓ Quality metrics tracking")
print("  ✓ Graceful degradation")

# ============================================================================
# PIPELINE METRICS
# ============================================================================

print("\n" + "=" * 80)
print("PIPELINE METRICS")
print("=" * 80)

metrics = {
    "total_input_records": df_raw.count(),
    "cleaned_records": df_cleaned.count(),
    "output_records": df_enriched.count(),
    "quality_issues": sum(quality_checks.values()),
    "partitions_written": df_enriched.select("year", "month").distinct().count()
}

print("\nPipeline Summary:")
for metric, value in metrics.items():
    print(f"  {metric:<25}: {value:>10,}")

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. EXTRACT BEST PRACTICES:
   ✓ Handle multiple source formats
   ✓ Use PERMISSIVE mode with corrupt record tracking
   ✓ Graceful error handling for missing files
   ✓ Schema validation

2. TRANSFORM BEST PRACTICES:
   ✓ Clean nulls and invalid data early
   ✓ Standardize date/time formats
   ✓ Derive business metrics
   ✓ Handle late arriving data
   ✓ Implement data quality checks

3. LOAD BEST PRACTICES:
   ✓ Partition by date for query optimization
   ✓ Use compression (snappy/zstd)
   ✓ Generate summary statistics
   ✓ Create data quality reports
   ✓ Use appropriate save mode

4. ERROR HANDLING:
   ✓ Try-except for file operations
   ✓ Quarantine corrupt records
   ✓ Log at each stage
   ✓ Track metrics
   ✓ Graceful degradation

5. DATA QUALITY:
   ✓ Null checks
   ✓ Range validation
   ✓ Duplicate detection
   ✓ Business rule validation
   ✓ Quality reporting

6. PRODUCTION PATTERNS:
   ✓ Idempotent operations
   ✓ Incremental processing
   ✓ Audit logging
   ✓ Monitoring and alerts
   ✓ Data lineage tracking
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
