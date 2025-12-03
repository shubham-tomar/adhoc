"""
Problem 3.3: Complex Window Operations
=======================================
Goal: Implement session windows and time-based analytics

Key Concepts:
- Session identification (time-based grouping)
- Session-level metrics
- Trend analysis over time
- Complex conditional logic with windows
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Problem 3.3 - Complex Window Operations") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 3.3: COMPLEX WINDOW OPERATIONS")
print("=" * 80)

# Read and prepare data
df = spark.read.parquet("transactions.parquet")
df = df.withColumn("transaction_date", to_date(col("transaction_date"))) \
       .withColumn("transaction_timestamp", 
                   to_timestamp(col("transaction_date")))

print("\nSample data:")
df.orderBy("customer_id", "transaction_timestamp").show(5)

# ============================================================================
# TASK 1: Session Identification
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: IDENTIFY SHOPPING SESSIONS")
print("=" * 80)

"""
Session Definition:
- Transactions by same customer within 7 days = same session
- Gap > 7 days = new session

Algorithm:
1. Calculate days since last transaction
2. Mark session start when gap > 7 days
3. Assign cumulative session ID
"""

print("\nIdentifying sessions (7-day window)...")

# Order transactions by customer and time
window_time = Window.partitionBy("customer_id").orderBy("transaction_timestamp")

# Step 1: Calculate days since previous transaction
df_with_gaps = df.withColumn(
    "prev_transaction_date",
    lag("transaction_date", 1).over(window_time)
).withColumn(
    "days_since_last",
    datediff(col("transaction_date"), lag("transaction_date", 1).over(window_time))
)

# Step 2: Mark session boundaries (gap > 7 days or first transaction)
SESSION_TIMEOUT_DAYS = 7

df_with_sessions = df_with_gaps.withColumn(
    "is_new_session",
    when(col("days_since_last").isNull(), 1)  # First transaction
    .when(col("days_since_last") > SESSION_TIMEOUT_DAYS, 1)  # Timeout
    .otherwise(0)
)

# Step 3: Assign session IDs (cumulative sum of session starts)
df_sessions = df_with_sessions.withColumn(
    "session_id",
    sum("is_new_session").over(
        Window.partitionBy("customer_id")
              .orderBy("transaction_timestamp")
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
).withColumn(
    "global_session_id",
    concat(col("customer_id"), lit("_S"), col("session_id").cast("string"))
)

print("\nSessions Identified:")
df_sessions.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "transaction_date", "days_since_last", 
            "is_new_session", "session_id", "global_session_id") \
    .orderBy("transaction_timestamp") \
    .show(15)

"""
Learning Points - Session Identification:

Key Technique: Cumulative sum of flags
1. Create flag for session start (1 or 0)
2. Cumulative sum of flags = session number
3. Each new flag increments the session ID

Why it works:
session_id = [1, 1, 1, 2, 2, 3, 3, 3]
            ↑        ↑     ↑
          Flag=1   Flag=1  Flag=1

Use cases:
- Shopping sessions
- User journeys
- Device sessions
- Campaign touchpoints
"""

# ============================================================================
# TASK 2: Session-Level Metrics
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: CALCULATE SESSION METRICS")
print("=" * 80)

"""
For each session, calculate:
- Total amount spent
- Number of items
- Session duration (first to last transaction)
- Transaction frequency
"""

print("\nCalculating session-level metrics...")

# Aggregate by session
session_metrics = df_sessions.groupBy("customer_id", "global_session_id", "session_id").agg(
    # Transaction metrics
    count("*").alias("transaction_count"),
    sum("quantity").alias("total_items"),
    sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction_amount"),
    
    # Time metrics
    min("transaction_timestamp").alias("session_start"),
    max("transaction_timestamp").alias("session_end"),
    
    # Product diversity
    countDistinct("category").alias("categories_browsed"),
    collect_set("category").alias("categories_list")
).withColumn(
    "session_duration_days",
    datediff(col("session_end"), col("session_start"))
).withColumn(
    "session_duration_hours",
    (unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))) / 3600
)

print("\nSession Metrics:")
session_metrics.orderBy("customer_id", "session_id").show(10, truncate=False)

# Session summary statistics
print("\nSession Summary Statistics:")
session_metrics.select(
    avg("transaction_count").alias("avg_transactions_per_session"),
    avg("total_spent").alias("avg_session_value"),
    avg("session_duration_days").alias("avg_session_duration_days"),
    max("transaction_count").alias("max_transactions_in_session")
).show()

"""
Session Metrics Use Cases:

E-commerce:
- Average basket size per session
- Session conversion rate
- Time to purchase

Mobile Apps:
- Session length
- Actions per session
- Feature usage per session

Marketing:
- Campaign touchpoints
- Customer journey mapping
- Attribution modeling
"""

# ============================================================================
# TASK 3: Customer Spending Trends
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: ANALYZE SPENDING TRENDS")
print("=" * 80)

"""
Compare first half vs second half of customer history
to identify increasing/decreasing spending trends
"""

print("\nAnalyzing spending trends (first half vs second half)...")

# Get each customer's transaction history split in half
window_customer = Window.partitionBy("customer_id").orderBy("transaction_timestamp")

df_with_sequence = df_sessions.withColumn(
    "transaction_number",
    row_number().over(window_customer)
).withColumn(
    "total_transactions",
    count("*").over(Window.partitionBy("customer_id"))
).withColumn(
    "is_first_half",
    when(col("transaction_number") <= (col("total_transactions") / 2), 1).otherwise(0)
)

# Aggregate by customer and half
customer_trends = df_with_sequence.groupBy("customer_id", "is_first_half").agg(
    sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction"),
    count("*").alias("transaction_count")
).groupBy("customer_id").pivot("is_first_half", [1, 0]).agg(
    first("total_spent").alias("total_spent"),
    first("avg_transaction").alias("avg_transaction"),
    first("transaction_count").alias("transaction_count")
)

# Calculate trend
customer_trends_calc = customer_trends.withColumn(
    "spending_change",
    col("0_total_spent") - col("1_total_spent")
).withColumn(
    "spending_change_pct",
    ((col("0_total_spent") - col("1_total_spent")) / col("1_total_spent") * 100)
).withColumn(
    "trend",
    when(col("spending_change_pct") > 20, "Strong Growth")
    .when(col("spending_change_pct") > 0, "Moderate Growth")
    .when(col("spending_change_pct") > -20, "Stable")
    .when(col("spending_change_pct") > -50, "Declining")
    .otherwise("Churning")
)

print("\nSpending Trends by Customer:")
customer_trends_calc.select(
    "customer_id",
    "1_total_spent",
    "0_total_spent",
    "spending_change",
    "spending_change_pct",
    "trend"
).orderBy(col("spending_change_pct").desc()).show(15)

# Trend distribution
print("\nTrend Distribution:")
customer_trends_calc.groupBy("trend").agg(
    count("*").alias("customer_count")
).orderBy("trend").show()

"""
Trend Analysis Applications:

Marketing:
- Target growth customers for upsell
- Re-engage declining customers
- Identify churn risk

Product:
- Validate feature improvements
- A/B test impact
- User lifecycle stages

Finance:
- Revenue forecasting
- LTV projections
- Customer value segmentation
"""

# ============================================================================
# TASK 4: Advanced Session Analysis
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: ADVANCED SESSION ANALYSIS")
print("=" * 80)

"""
Analyze session patterns:
- Single-transaction sessions vs multi-transaction
- Session value distribution
- Category progression within sessions
"""

print("\n1. Session Type Classification:")

# Classify sessions
session_classified = session_metrics.withColumn(
    "session_type",
    when(col("transaction_count") == 1, "Single Transaction")
    .when(col("transaction_count") <= 3, "Small Session")
    .when(col("transaction_count") <= 10, "Medium Session")
    .otherwise("Large Session")
).withColumn(
    "value_tier",
    when(col("total_spent") > 1000, "High Value")
    .when(col("total_spent") > 500, "Medium Value")
    .otherwise("Low Value")
)

print("\nSession Classification:")
session_classified.groupBy("session_type", "value_tier").agg(
    count("*").alias("session_count"),
    avg("total_spent").alias("avg_value"),
    avg("transaction_count").alias("avg_transactions")
).orderBy("session_type", "value_tier").show()

print("\n2. Product Journey in Sessions:")

# Get category sequence within each session
category_sequence = df_sessions.withColumn(
    "transaction_order",
    row_number().over(
        Window.partitionBy("global_session_id")
              .orderBy("transaction_timestamp")
    )
).select(
    "global_session_id",
    "customer_id",
    "category",
    "transaction_order",
    "amount"
).groupBy("global_session_id", "customer_id").agg(
    collect_list(
        struct("transaction_order", "category", "amount")
    ).alias("journey")
).withColumn(
    "journey_length",
    size("journey")
).withColumn(
    "first_category",
    element_at(col("journey"), 1).getField("category")
).withColumn(
    "last_category",
    element_at(col("journey"), -1).getField("category")
)

print("\nSession Journeys (multi-transaction sessions):")
category_sequence.filter(col("journey_length") > 1) \
    .select("customer_id", "journey_length", "first_category", "last_category") \
    .show(10)

# ============================================================================
# TASK 5: Time-Based Cohort Analysis
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: TIME-BASED COHORT ANALYSIS")
print("=" * 80)

"""
Cohort Analysis:
- Group customers by first purchase month
- Track behavior over time
- Calculate retention and LTV
"""

print("\nPerforming cohort analysis...")

# Define cohort (month of first purchase)
window_first = Window.partitionBy("customer_id").orderBy("transaction_timestamp")

df_cohort = df_sessions.withColumn(
    "first_purchase_date",
    first_value("transaction_date").over(window_first)
).withColumn(
    "cohort_month",
    date_format(first_value("transaction_date").over(window_first), "yyyy-MM")
).withColumn(
    "months_since_first",
    months_between(col("transaction_date"), first_value("transaction_date").over(window_first)).cast("int")
)

# Cohort metrics
cohort_metrics = df_cohort.groupBy("cohort_month", "months_since_first").agg(
    countDistinct("customer_id").alias("active_customers"),
    sum("amount").alias("revenue"),
    count("*").alias("transactions")
).orderBy("cohort_month", "months_since_first")

print("\nCohort Analysis (First 3 months):")
cohort_metrics.filter(col("months_since_first") <= 3).show(20)

# Calculate retention rate
cohort_initial = df_cohort.groupBy("cohort_month").agg(
    countDistinct("customer_id").alias("cohort_size")
)

cohort_retention = cohort_metrics.join(cohort_initial, "cohort_month") \
    .withColumn(
        "retention_rate",
        (col("active_customers") / col("cohort_size") * 100)
    )

print("\nRetention Rates by Cohort:")
cohort_retention.filter(col("months_since_first").isin([0, 1, 2, 3])) \
    .select("cohort_month", "months_since_first", "cohort_size", 
            "active_customers", "retention_rate") \
    .orderBy("cohort_month", "months_since_first") \
    .show(20)

"""
Cohort Analysis Insights:

Month 0: 100% (all customers start)
Month 1: ~40-60% typical retention
Month 3: ~20-30% typical retention
Month 12: ~10-20% typical retention

Good cohorts:
✓ High month-1 retention (>50%)
✓ Stable retention curve
✓ Increasing LTV over time

Bad cohorts:
✗ Steep drop after month 1
✗ Low engagement
✗ Decreasing transaction value
"""

# ============================================================================
# TASK 6: Rolling Windows with Range
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: RANGE-BASED WINDOWS")
print("=" * 80)

"""
rangeBetween: Define window by value range instead of row count

Example: "All transactions within last 30 days"
"""

print("\nCalculating metrics over 30-day rolling window...")

# Convert date to unix timestamp for range calculations
df_with_unix = df_sessions.withColumn(
    "unix_date",
    unix_timestamp(col("transaction_date"))
)

# 30-day window (in seconds)
THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60

window_30_days = Window.partitionBy("customer_id") \
                       .orderBy("unix_date") \
                       .rangeBetween(-THIRTY_DAYS_SECONDS, 0)

df_rolling = df_with_unix.withColumn(
    "transactions_last_30d",
    count("*").over(window_30_days)
).withColumn(
    "spending_last_30d",
    sum("amount").over(window_30_days)
).withColumn(
    "avg_transaction_30d",
    avg("amount").over(window_30_days)
)

print("\n30-Day Rolling Metrics:")
df_rolling.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "transaction_date", "amount",
            "transactions_last_30d", "spending_last_30d", "avg_transaction_30d") \
    .orderBy("transaction_date") \
    .show(15)

"""
rowsBetween vs rangeBetween:

rowsBetween(-3, 0):
✓ Last 4 rows (including current)
✓ Fixed window size
✓ Fast performance

rangeBetween(-30_days, 0):
✓ All rows within 30 days
✓ Variable window size
✓ More accurate for time-based analysis
⚠️  Requires numeric/timestamp orderBy column
"""

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. SESSION IDENTIFICATION:
   ✓ Use lag() to calculate time gaps
   ✓ Flag session starts (first or timeout)
   ✓ Cumulative sum of flags = session ID
   ✓ Works for any time-based grouping

2. SESSION METRICS:
   ✓ Aggregate by session ID
   ✓ Calculate duration, value, actions
   ✓ Classify sessions by behavior
   ✓ Track product journey

3. TREND ANALYSIS:
   ✓ Split timeline (first half vs second half)
   ✓ Use pivot for comparison
   ✓ Calculate growth/decline rates
   ✓ Segment by trend category

4. COHORT ANALYSIS:
   ✓ Group by first activity date
   ✓ Track metrics over time
   ✓ Calculate retention rates
   ✓ Identify successful cohorts

5. COMPLEX WINDOWS:
   ✓ Combine multiple window specs
   ✓ Use rangeBetween for time-based analysis
   ✓ Nest window functions
   ✓ Cumulative calculations

6. PRACTICAL APPLICATIONS:
   ✓ E-commerce: Shopping sessions, cart analysis
   ✓ SaaS: User sessions, feature adoption
   ✓ Mobile: App sessions, user journeys
   ✓ Marketing: Campaign attribution, touchpoints

7. PERFORMANCE TIPS:
   ✓ Reuse window specifications
   ✓ Filter data before windowing
   ✓ Use rowsBetween when possible (faster than rangeBetween)
   ✓ Cache intermediate results
   ✓ Partition data appropriately

8. COMMON PATTERNS:
   ✓ Session ID: cumsum(session_start_flag)
   ✓ Trend: first_half vs second_half comparison
   ✓ Cohort: group by first_activity_month
   ✓ Rolling metrics: rangeBetween(-N_days, 0)
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
print("\n💡 Session analysis is powerful for understanding user behavior patterns!")
