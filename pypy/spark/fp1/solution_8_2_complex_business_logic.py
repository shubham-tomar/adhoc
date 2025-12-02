"""
Problem 8.2: Complex Business Logic - Session Analysis
=======================================================
Goal: Implement session analysis with complex business rules

Key Concepts:
- Session identification with time gaps
- Complex windowing
- User journey tracking
- Pattern analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("Problem 8.2 - Complex Business Logic") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 8.2: COMPLEX SESSION ANALYSIS")
print("=" * 80)

# Prepare data with timestamps
df = spark.read.parquet("transactions.parquet")
df = df.withColumn("transaction_date", to_date(col("transaction_date"))) \
       .withColumn("transaction_timestamp",
                   to_timestamp(concat(
                       col("transaction_date"),
                       lit(" "),
                       lpad((rand() * 24).cast("int").cast("string"), 2, "0"),
                       lit(":"),
                       lpad((rand() * 60).cast("int").cast("string"), 2, "0"),
                       lit(":00")
                   )))

print(f"\nDataset: {df.count()} records")

# ============================================================================
# TASK 1: Define Shopping Sessions
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: SESSION IDENTIFICATION (30-minute gap)")
print("=" * 80)

"""
Session Definition:
- Transactions within 30 minutes = same session
- Gap > 30 minutes = new session
- Per customer
"""

SESSION_TIMEOUT_MINUTES = 30

window_time = Window.partitionBy("customer_id").orderBy("transaction_timestamp")

# Calculate time since last transaction
df_with_gaps = df.withColumn(
    "prev_timestamp",
    lag("transaction_timestamp", 1).over(window_time)
).withColumn(
    "minutes_since_last",
    (unix_timestamp("transaction_timestamp") - unix_timestamp("prev_timestamp")) / 60
)

# Mark session boundaries
df_with_sessions = df_with_gaps.withColumn(
    "is_new_session",
    when(col("minutes_since_last").isNull(), 1)  # First transaction
    .when(col("minutes_since_last") > SESSION_TIMEOUT_MINUTES, 1)  # Timeout
    .otherwise(0)
)

# Assign session IDs (cumulative sum)
df_sessions = df_with_sessions.withColumn(
    "session_num",
    sum("is_new_session").over(
        Window.partitionBy("customer_id")
              .orderBy("transaction_timestamp")
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
).withColumn(
    "session_id",
    concat(col("customer_id"), lit("_S"), col("session_num").cast("string"))
)

print("\nSession Example (Customer CUST0001):")
df_sessions.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "transaction_timestamp", "minutes_since_last",
            "is_new_session", "session_id") \
    .orderBy("transaction_timestamp") \
    .show(15, truncate=False)

session_count = df_sessions.select("session_id").distinct().count()
print(f"\nTotal sessions identified: {session_count}")

# ============================================================================
# TASK 2: Calculate Session Metrics
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: SESSION-LEVEL METRICS")
print("=" * 80)

session_metrics = df_sessions.groupBy("customer_id", "session_id").agg(
    # Time metrics
    min("transaction_timestamp").alias("session_start"),
    max("transaction_timestamp").alias("session_end"),
    
    # Transaction metrics
    count("*").alias("transaction_count"),
    sum("quantity").alias("total_items"),
    sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction_amount"),
    
    # Product diversity
    countDistinct("category").alias("categories_browsed"),
    countDistinct("product_id").alias("products_viewed"),
    
    # Behavioral
    collect_list("category").alias("category_sequence")
).withColumn(
    "session_duration_minutes",
    (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60
).withColumn(
    "session_conversion",
    when(col("total_spent") > 50, "Converted").otherwise("Abandoned")
)

print("\nSession Metrics Summary:")
session_metrics.describe("transaction_count", "total_spent", "session_duration_minutes").show()

print("\nSample Session Metrics:")
session_metrics.orderBy(col("total_spent").desc()).show(10, truncate=False)

# ============================================================================
# TASK 3: User Journey Analysis
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: USER JOURNEY PATTERNS")
print("=" * 80)

"""
Analyze sequence of categories in successful sessions
"""

# Create journey string from category sequence
df_journeys = session_metrics.withColumn(
    "journey",
    array_join("category_sequence", " → ")
).select("session_id", "customer_id", "journey", "session_conversion", "total_spent")

print("\nSample User Journeys:")
df_journeys.orderBy(col("total_spent").desc()).show(10, truncate=False)

# Common journey patterns
print("\nMost Common Journey Patterns:")
journey_patterns = df_journeys.groupBy("journey", "session_conversion").agg(
    count("*").alias("occurrence_count"),
    avg("total_spent").alias("avg_spent")
).orderBy(col("occurrence_count").desc())

journey_patterns.show(15, truncate=False)

# ============================================================================
# TASK 4: Session Conversion Analysis
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: SESSION CONVERSION ANALYSIS")
print("=" * 80)

"""
Compare successful vs abandoned sessions
"""

conversion_comparison = session_metrics.groupBy("session_conversion").agg(
    count("*").alias("session_count"),
    avg("transaction_count").alias("avg_transactions"),
    avg("total_items").alias("avg_items"),
    avg("total_spent").alias("avg_spent"),
    avg("session_duration_minutes").alias("avg_duration_min"),
    avg("categories_browsed").alias("avg_categories"),
    avg("products_viewed").alias("avg_products")
)

print("\nConversion Comparison:")
conversion_comparison.show(truncate=False)

# Calculate conversion rate
total_sessions = session_metrics.count()
converted_sessions = session_metrics.filter(col("session_conversion") == "Converted").count()
conversion_rate = (converted_sessions / total_sessions) * 100

print(f"\nConversion Metrics:")
print(f"  Total sessions: {total_sessions}")
print(f"  Converted: {converted_sessions}")
print(f"  Conversion rate: {conversion_rate:.2f}%")

# ============================================================================
# TASK 5: Advanced Pattern Detection
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: PATTERN DETECTION")
print("=" * 80)

"""
Identify patterns that predict conversion
"""

# Pattern 1: First category impact
first_category = df_sessions.withColumn(
    "first_category",
    first_value("category").over(
        Window.partitionBy("session_id")
              .orderBy("transaction_timestamp")
    )
).groupBy("session_id", "customer_id").agg(
    first("first_category").alias("entry_category")
).join(
    session_metrics.select("session_id", "session_conversion", "total_spent"),
    "session_id"
)

print("\n1. Entry Category Impact on Conversion:")
entry_analysis = first_category.groupBy("entry_category", "session_conversion").agg(
    count("*").alias("sessions"),
    avg("total_spent").alias("avg_spent")
).orderBy("entry_category", "session_conversion")

entry_analysis.show(truncate=False)

# Pattern 2: Time of day analysis
session_with_hour = session_metrics.withColumn(
    "hour_of_day",
    hour("session_start")
).withColumn(
    "time_period",
    when(col("hour_of_day").between(0, 5), "Night")
    .when(col("hour_of_day").between(6, 11), "Morning")
    .when(col("hour_of_day").between(12, 17), "Afternoon")
    .otherwise("Evening")
)

print("\n2. Time of Day Impact:")
time_analysis = session_with_hour.groupBy("time_period").agg(
    count("*").alias("sessions"),
    sum(when(col("session_conversion") == "Converted", 1).otherwise(0)).alias("converted"),
    avg("total_spent").alias("avg_spent")
).withColumn(
    "conversion_rate",
    (col("converted") / col("sessions") * 100).cast("decimal(5,2)")
)

time_analysis.orderBy("time_period").show()

# Pattern 3: Multi-category sessions
category_diversity = session_metrics.withColumn(
    "diversity_level",
    when(col("categories_browsed") == 1, "Single")
    .when(col("categories_browsed") <= 2, "Low")
    .when(col("categories_browsed") <= 3, "Medium")
    .otherwise("High")
)

print("\n3. Category Diversity Impact:")
diversity_analysis = category_diversity.groupBy("diversity_level").agg(
    count("*").alias("sessions"),
    sum(when(col("session_conversion") == "Converted", 1).otherwise(0)).alias("converted"),
    avg("total_spent").alias("avg_spent")
).withColumn(
    "conversion_rate",
    (col("converted") / col("sessions") * 100).cast("decimal(5,2)")
)

diversity_analysis.show()

# ============================================================================
# TASK 6: Customer Segmentation by Behavior
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: BEHAVIORAL SEGMENTATION")
print("=" * 80)

"""
Segment customers based on session behavior
"""

customer_behavior = session_metrics.groupBy("customer_id").agg(
    count("*").alias("total_sessions"),
    sum(when(col("session_conversion") == "Converted", 1).otherwise(0)).alias("converted_sessions"),
    avg("transaction_count").alias("avg_txns_per_session"),
    avg("total_spent").alias("avg_session_value"),
    avg("session_duration_minutes").alias("avg_session_duration"),
    max("total_spent").alias("max_session_value")
).withColumn(
    "conversion_rate",
    (col("converted_sessions") / col("total_sessions") * 100).cast("decimal(5,2)")
)

# Segment customers
customer_segments = customer_behavior.withColumn(
    "segment",
    when(
        (col("conversion_rate") >= 50) & (col("avg_session_value") >= 100),
        "Champions"
    ).when(
        (col("conversion_rate") >= 30) & (col("total_sessions") >= 3),
        "Engaged"
    ).when(
        (col("total_sessions") >= 5) & (col("conversion_rate") < 20),
        "Browsers"
    ).when(
        col("total_sessions") == 1,
        "New"
    ).otherwise("At Risk")
)

print("\nCustomer Segments:")
segment_summary = customer_segments.groupBy("segment").agg(
    count("*").alias("customer_count"),
    avg("conversion_rate").alias("avg_conversion"),
    avg("avg_session_value").alias("avg_value"),
    avg("total_sessions").alias("avg_sessions")
)

segment_summary.orderBy("segment").show()

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. SESSION IDENTIFICATION:
   ✓ Use time gaps to define sessions
   ✓ Cumulative sum for session IDs
   ✓ Per-user session tracking
   ✓ Configurable timeout threshold

2. SESSION METRICS:
   ✓ Duration, transaction count
   ✓ Items, spending
   ✓ Product diversity
   ✓ Conversion status

3. USER JOURNEY:
   ✓ Category sequence tracking
   ✓ Entry point analysis
   ✓ Path to conversion
   ✓ Common patterns

4. CONVERSION ANALYSIS:
   ✓ Compare successful vs abandoned
   ✓ Identify conversion drivers
   ✓ Time of day impact
   ✓ Category diversity effect

5. PATTERN DETECTION:
   ✓ Entry category impact
   ✓ Time period analysis
   ✓ Diversity correlation
   ✓ Behavioral indicators

6. CUSTOMER SEGMENTATION:
   ✓ Segment by behavior
   ✓ Conversion rate based
   ✓ Session value tiers
   ✓ Engagement levels

7. APPLICATIONS:
   ✓ Personalization
   ✓ Recommendation engines
   ✓ Marketing campaigns
   ✓ A/B testing insights
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Session analysis = Understanding user behavior!")
