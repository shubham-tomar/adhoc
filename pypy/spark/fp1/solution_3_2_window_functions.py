"""
Problem 3.2: Window Functions - Running Totals and Rankings
============================================================
Goal: Use window functions for complex analytics

Key Concepts:
- Window specifications (partitionBy, orderBy)
- Ranking functions (row_number, rank, dense_rank)
- Analytical functions (lag, lead, running totals)
- Frame specifications (rows between, range between)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Problem 3.2 - Window Functions") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 3.2: WINDOW FUNCTIONS - RUNNING TOTALS AND RANKINGS")
print("=" * 80)

# Read data
df = spark.read.parquet("transactions.parquet")

# Convert transaction_date to proper date type
df = df.withColumn("transaction_date", to_date(col("transaction_date")))

print("\nSample data:")
df.orderBy("customer_id", "transaction_date").show(5)

# ============================================================================
# TASK 1: Running Total of Spending Over Time
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: RUNNING TOTAL (Cumulative Sum)")
print("=" * 80)

"""
Window Function Anatomy:

Window.partitionBy("customer_id")     ← Group by customer
      .orderBy("transaction_date")    ← Order within group
      .rowsBetween(start, end)        ← Frame specification

Frame Types:
- rowsBetween: Physical rows (count-based)
- rangeBetween: Logical range (value-based)
"""

print("\nCalculating running total per customer...")

# Define window: partition by customer, order by date
window_running = Window.partitionBy("customer_id") \
                       .orderBy("transaction_date") \
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running_total = df.withColumn(
    "running_total",
    sum("amount").over(window_running)
).withColumn(
    "transaction_number",
    row_number().over(Window.partitionBy("customer_id").orderBy("transaction_date"))
)

print("\nRunning Total by Customer:")
df_running_total.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "transaction_date", "amount", "transaction_number", "running_total") \
    .orderBy("transaction_date") \
    .show(10)

"""
Learning Points - Running Total:

Window Frame:
- unboundedPreceding: Start of partition
- currentRow: Current row
- unboundedFollowing: End of partition

sum().over(window):
- Calculates sum from start to current row
- Creates cumulative/running total
- Useful for: Lifetime value, YTD metrics
"""

# ============================================================================
# TASK 2: Ranking Transactions by Amount
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: RANKING FUNCTIONS")
print("=" * 80)

"""
Three Ranking Functions:

1. row_number(): 1, 2, 3, 4, ... (always unique, arbitrary for ties)
2. rank():       1, 2, 2, 4, ... (gaps after ties)
3. dense_rank(): 1, 2, 2, 3, ... (no gaps)
"""

print("\nComparing ranking functions...")

# Window for ranking within each customer
window_rank = Window.partitionBy("customer_id").orderBy(col("amount").desc())

df_ranked = df.withColumn("row_num", row_number().over(window_rank)) \
              .withColumn("rank", rank().over(window_rank)) \
              .withColumn("dense_rank", dense_rank().over(window_rank))

print("\nRanking Example (Customer CUST0001's transactions):")
df_ranked.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "amount", "row_num", "rank", "dense_rank") \
    .orderBy("amount", ascending=False) \
    .show(10)

"""
When to use each:

row_number():
✓ Need unique ranks (e.g., top 1 per group)
✓ Arbitrary tie-breaking acceptable
✓ Pagination

rank():
✓ Show gaps after ties (e.g., Olympic medals)
✓ Traditional ranking
✓ "Skip ranks" for ties

dense_rank():
✓ No gaps in ranking
✓ Consecutive ranks
✓ Leaderboards
"""

# ============================================================================
# TASK 3: Difference from Previous Transaction (LAG)
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: LAG and LEAD Functions")
print("=" * 80)

"""
lag(col, offset):  Access previous row's value
lead(col, offset): Access next row's value

offset = 1: Previous/next row
offset = 2: Two rows back/ahead
"""

print("\nCalculating change from previous transaction...")

window_ordered = Window.partitionBy("customer_id").orderBy("transaction_date")

df_with_lag = df.withColumn(
    "previous_amount",
    lag("amount", 1).over(window_ordered)
).withColumn(
    "amount_change",
    col("amount") - lag("amount", 1).over(window_ordered)
).withColumn(
    "amount_change_pct",
    ((col("amount") - lag("amount", 1).over(window_ordered)) / 
     lag("amount", 1).over(window_ordered) * 100)
).withColumn(
    "next_amount",
    lead("amount", 1).over(window_ordered)
)

print("\nLag/Lead Example:")
df_with_lag.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "transaction_date", "previous_amount", 
            "amount", "next_amount", "amount_change", "amount_change_pct") \
    .orderBy("transaction_date") \
    .show(10)

"""
Use Cases:

lag():
✓ Calculate change from previous value
✓ Detect increasing/decreasing trends
✓ Time-series analysis
✓ Churn detection (days since last purchase)

lead():
✓ Look-ahead analysis
✓ Predict next value
✓ Calculate time until next event
"""

# ============================================================================
# TASK 4: Moving Average (Last 3 Transactions)
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: MOVING AVERAGE (Rolling Window)")
print("=" * 80)

"""
Moving Average Frame:
- rowsBetween(-2, 0): Last 3 rows (including current)
- rowsBetween(-6, 0): Last 7 rows
- rangeBetween(-7days, 0): Last 7 days of data
"""

print("\nCalculating 3-transaction moving average...")

# Last 3 transactions (including current)
window_moving_3 = Window.partitionBy("customer_id") \
                        .orderBy("transaction_date") \
                        .rowsBetween(-2, 0)

df_moving_avg = df.withColumn(
    "moving_avg_3",
    avg("amount").over(window_moving_3)
).withColumn(
    "moving_sum_3",
    sum("amount").over(window_moving_3)
).withColumn(
    "count_in_window",
    count("*").over(window_moving_3)
)

print("\nMoving Average Example:")
df_moving_avg.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "transaction_date", "amount", 
            "count_in_window", "moving_avg_3", "moving_sum_3") \
    .orderBy("transaction_date") \
    .show(10)

"""
Frame Specifications:

Physical (rowsBetween):
- Based on row count
- rowsBetween(-2, 0): Last 3 rows
- Always includes exact N rows (if available)

Logical (rangeBetween):
- Based on column value
- rangeBetween(-7, 0): Last 7 days
- Variable number of rows
- Requires numeric or date orderBy column

Common Patterns:
- Moving average: avg().over(rowsBetween(-N, 0))
- Expanding window: rowsBetween(unboundedPreceding, currentRow)
- Centered window: rowsBetween(-N, N)
"""

# ============================================================================
# TASK 5: Top 3 Transactions per Category
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: TOP N PER GROUP")
print("=" * 80)

"""
Common Pattern: Find top N items per group
1. Rank within each group
2. Filter rank <= N
"""

print("\nFinding top 3 highest transactions per category...")

# Window partitioned by category
window_category = Window.partitionBy("category").orderBy(col("amount").desc())

top_per_category = df.withColumn(
    "rank",
    row_number().over(window_category)
).filter(col("rank") <= 3) \
 .select("category", "transaction_id", "customer_id", "amount", "rank")

print("\nTop 3 Transactions per Category:")
top_per_category.orderBy("category", "rank").show(20)

# Alternative: Using dense_rank for ties
print("\nWith dense_rank (includes ties):")
top_with_ties = df.withColumn(
    "dense_rank",
    dense_rank().over(window_category)
).filter(col("dense_rank") <= 3) \
 .select("category", "amount", "dense_rank")

top_with_ties.groupBy("category").count().show()

"""
Top-N Pattern Use Cases:
✓ Top products per category
✓ Top customers per region
✓ Best performers per department
✓ High-value transactions per day
"""

# ============================================================================
# TASK 6: Percentile Rank
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: PERCENTILE RANK")
print("=" * 80)

"""
percent_rank(): Returns percentile rank (0.0 to 1.0)
ntile(n): Divides rows into n buckets
"""

print("\nCalculating percentile rank by region...")

window_region = Window.partitionBy("region").orderBy("amount")

df_percentile = df.withColumn(
    "percent_rank",
    percent_rank().over(window_region)
).withColumn(
    "percentile",
    (percent_rank().over(window_region) * 100).cast("int")
).withColumn(
    "quartile",
    ntile(4).over(window_region)
).withColumn(
    "decile",
    ntile(10).over(window_region)
)

print("\nPercentile Ranks (Top transactions in each region):")
df_percentile.filter(col("percent_rank") >= 0.95) \
    .select("region", "amount", "percent_rank", "percentile", "quartile", "decile") \
    .orderBy("region", col("amount").desc()) \
    .show(20)

"""
Percentile Functions:

percent_rank():
- Returns position as percentage (0.0 - 1.0)
- Formula: (rank - 1) / (rows - 1)
- Use: Identify top/bottom X%

ntile(n):
- Divides into n equal buckets
- Returns bucket number (1 to n)
- Use: Quartiles, deciles, percentile groups

cume_dist():
- Cumulative distribution
- Percentage of values <= current value
"""

# ============================================================================
# TASK 7: First and Last Value
# ============================================================================

print("\n" + "=" * 80)
print("TASK 7: FIRST AND LAST VALUE")
print("=" * 80)

"""
first_value(): First value in window
last_value(): Last value in window

Useful for:
- Baseline comparison
- Start/end metrics
- Delta calculations
"""

print("\nComparing each transaction to customer's first and last...")

window_customer = Window.partitionBy("customer_id").orderBy("transaction_date")

# Need to specify frame for last_value to work correctly
window_customer_full = Window.partitionBy("customer_id") \
                             .orderBy("transaction_date") \
                             .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_first_last = df.withColumn(
    "first_amount",
    first_value("amount").over(window_customer)
).withColumn(
    "last_amount",
    last_value("amount").over(window_customer_full)
).withColumn(
    "vs_first_purchase",
    col("amount") - first_value("amount").over(window_customer)
).withColumn(
    "first_date",
    first_value("transaction_date").over(window_customer)
).withColumn(
    "days_since_first",
    datediff(col("transaction_date"), first_value("transaction_date").over(window_customer))
)

print("\nFirst/Last Value Example:")
df_first_last.filter(col("customer_id") == "CUST0001") \
    .select("customer_id", "transaction_date", "amount", 
            "first_amount", "last_amount", "vs_first_purchase", "days_since_first") \
    .orderBy("transaction_date") \
    .show(10)

"""
Important: Frame for last_value()

Without proper frame, last_value() returns current row!

Wrong:
window = Window.partitionBy("id").orderBy("date")
last_value("col").over(window)  # Returns current row!

Correct:
window = Window.partitionBy("id").orderBy("date")
              .rowsBetween(unboundedPreceding, unboundedFollowing)
last_value("col").over(window)  # Returns actual last value
"""

# ============================================================================
# TASK 8: Complex Analytics - Customer Spending Trends
# ============================================================================

print("\n" + "=" * 80)
print("TASK 8: COMPREHENSIVE ANALYTICS")
print("=" * 80)

"""
Combine multiple window functions for rich analytics
"""

print("\nBuilding comprehensive transaction analytics...")

# Multiple windows for different purposes
window_time = Window.partitionBy("customer_id").orderBy("transaction_date")
window_amount = Window.partitionBy("customer_id").orderBy(col("amount").desc())

comprehensive = df.withColumn(
    # Time-based metrics
    "transaction_seq", row_number().over(window_time)
).withColumn(
    "running_total", sum("amount").over(window_time.rowsBetween(Window.unboundedPreceding, 0))
).withColumn(
    "moving_avg_3", avg("amount").over(window_time.rowsBetween(-2, 0))
).withColumn(
    "days_since_last", 
    datediff(col("transaction_date"), lag("transaction_date", 1).over(window_time))
).withColumn(
    # Amount-based metrics
    "amount_rank", row_number().over(window_amount)
).withColumn(
    "is_top_3", when(row_number().over(window_amount) <= 3, "Yes").otherwise("No")
).withColumn(
    # Trend indicators
    "trend",
    when(col("amount") > lag("amount", 1).over(window_time), "Increasing")
    .when(col("amount") < lag("amount", 1).over(window_time), "Decreasing")
    .otherwise("Stable")
)

print("\nComprehensive Analytics:")
comprehensive.filter(col("customer_id") == "CUST0001") \
    .select("transaction_date", "amount", "transaction_seq", "running_total", 
            "moving_avg_3", "days_since_last", "amount_rank", "trend") \
    .orderBy("transaction_date") \
    .show(15)

# ============================================================================
# TASK 9: Performance Considerations
# ============================================================================

print("\n" + "=" * 80)
print("TASK 9: PERFORMANCE TIPS")
print("=" * 80)

"""
Window Function Performance:

FAST (Non-shuffling):
✓ row_number(), rank(), dense_rank()
✓ lag(), lead()
✓ first_value(), last_value()

SLOW (May require shuffle):
⚠️  Aggregations: sum(), avg(), count()
⚠️  If data not partitioned correctly

Optimization:
1. Reuse window specs
2. Combine operations in single pass
3. Filter before window operations
4. Cache if using multiple times
"""

# Bad: Define window multiple times
print("\n❌ ANTI-PATTERN: Redefining windows")
print("df.withColumn('a', sum('x').over(Window.partitionBy('id')))")
print("  .withColumn('b', avg('x').over(Window.partitionBy('id')))")
print("  → Creates multiple passes over data!")

# Good: Reuse window spec
print("\n✅ BEST PRACTICE: Reuse window specification")
print("window = Window.partitionBy('id')")
print("df.withColumn('a', sum('x').over(window))")
print("  .withColumn('b', avg('x').over(window))")
print("  → Single pass over data!")

# Demonstrate
window_reuse = Window.partitionBy("customer_id").orderBy("transaction_date")

efficient = df.withColumn("running_total", sum("amount").over(window_reuse)) \
              .withColumn("running_count", count("*").over(window_reuse)) \
              .withColumn("running_avg", avg("amount").over(window_reuse))

print("\nEfficient window reuse:")
efficient.filter(col("customer_id") == "CUST0001").show(5)

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. WINDOW SPECIFICATIONS:
   ✓ partitionBy: Group data
   ✓ orderBy: Order within partition
   ✓ rowsBetween/rangeBetween: Define frame

2. RANKING FUNCTIONS:
   ✓ row_number(): Unique ranks
   ✓ rank(): With gaps for ties
   ✓ dense_rank(): No gaps
   ✓ percent_rank(): Percentile (0-1)
   ✓ ntile(n): Divide into buckets

3. ANALYTICAL FUNCTIONS:
   ✓ lag/lead: Access adjacent rows
   ✓ first_value/last_value: Boundary values
   ✓ Aggregations: sum, avg, count, etc.

4. FRAME SPECIFICATIONS:
   ✓ unboundedPreceding: Start of partition
   ✓ unboundedFollowing: End of partition
   ✓ currentRow: Current row
   ✓ rowsBetween(-N, 0): Last N rows
   ✓ rangeBetween: Value-based range

5. COMMON PATTERNS:
   ✓ Running total: sum().over(rowsBetween(unbounded, current))
   ✓ Moving average: avg().over(rowsBetween(-N, 0))
   ✓ Top N per group: rank + filter
   ✓ Change from previous: lag()
   ✓ Percentile rank: percent_rank()

6. PERFORMANCE:
   ✓ Reuse window specifications
   ✓ Combine multiple operations
   ✓ Filter before windowing
   ✓ Cache for multiple window operations
   ⚠️  Window functions can be expensive

7. USE CASES:
   ✓ Time-series analysis
   ✓ Running metrics (YTD, MTD)
   ✓ Ranking and top-N
   ✓ Trend detection
   ✓ Cohort analysis
   ✓ Customer journey analytics
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
