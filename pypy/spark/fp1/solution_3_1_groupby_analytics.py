"""
Problem 3.1: Customer Analytics with GroupBy
=============================================
Goal: Calculate various customer metrics using groupBy operations

Key Concepts:
- GroupBy aggregations
- Multiple aggregation functions
- Mode and most frequent value
- Complex aggregations with multiple columns
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Problem 3.1 - GroupBy Analytics") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 3.1: CUSTOMER ANALYTICS WITH GROUPBY")
print("=" * 80)

# Read data
df = spark.read.parquet("transactions.parquet")

print(f"\nDataset Overview:")
print(f"  Total transactions: {df.count()}")
print(f"  Unique customers: {df.select('customer_id').distinct().count()}")

df.show(5)

# ============================================================================
# TASK 1: Basic Customer Metrics
# ============================================================================

print("\n" + "=" * 80)
print("TASK 1: BASIC CUSTOMER METRICS")
print("=" * 80)

"""
Calculate for each customer:
1. Total spending
2. Average transaction amount
3. Number of transactions
4. Min and max transaction amounts
"""

print("\nCalculating customer metrics...")

customer_metrics = df.groupBy("customer_id").agg(
    # Total spending
    sum("amount").alias("total_spending"),
    
    # Average transaction
    avg("amount").alias("avg_transaction"),
    
    # Number of transactions
    count("*").alias("transaction_count"),
    
    # Min and max amounts
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount"),
    
    # Standard deviation (spending variability)
    stddev("amount").alias("spending_stddev")
)

print("\nCustomer Metrics:")
customer_metrics.orderBy(col("total_spending").desc()).show(10)

"""
Learning Points:

Aggregation Functions:
- sum():    Total/sum of values
- avg():    Mean/average
- count():  Number of rows
- min():    Minimum value
- max():    Maximum value
- stddev(): Standard deviation
- variance(): Variance
- collect_list(): Collect values into array
- collect_set(): Collect unique values into array
"""

# ============================================================================
# TASK 2: Most Frequent Category per Customer
# ============================================================================

print("\n" + "=" * 80)
print("TASK 2: MOST FREQUENT CATEGORY (Mode)")
print("=" * 80)

"""
Find the most frequently purchased category for each customer

Approach:
1. Group by customer + category
2. Count purchases per category
3. Window function to rank categories
4. Take top 1
"""

print("\nFinding most frequent category per customer...")

# Step 1: Count purchases per customer-category
category_counts = df.groupBy("customer_id", "category").agg(
    count("*").alias("category_count")
)

# Step 2: Rank categories per customer
window_spec = Window.partitionBy("customer_id").orderBy(col("category_count").desc())

category_ranked = category_counts.withColumn(
    "rank",
    row_number().over(window_spec)
)

# Step 3: Get most frequent (rank = 1)
most_frequent_category = category_ranked.filter(col("rank") == 1) \
    .select("customer_id", col("category").alias("favorite_category"), "category_count")

print("\nMost Frequent Category per Customer:")
most_frequent_category.show(10)

"""
Alternative Approach: Using mode() aggregate function
(Available in Spark 3.4+)
"""

# ============================================================================
# TASK 3: Most Frequent Payment Method
# ============================================================================

print("\n" + "=" * 80)
print("TASK 3: MOST FREQUENT PAYMENT METHOD")
print("=" * 80)

print("\nFinding preferred payment method per customer...")

# Similar approach for payment method
payment_counts = df.groupBy("customer_id", "payment_method").agg(
    count("*").alias("payment_count")
)

window_payment = Window.partitionBy("customer_id").orderBy(col("payment_count").desc())

most_frequent_payment = payment_counts.withColumn(
    "rank",
    row_number().over(window_payment)
).filter(col("rank") == 1) \
 .select("customer_id", col("payment_method").alias("preferred_payment"), "payment_count")

print("\nMost Frequent Payment Method:")
most_frequent_payment.show(10)

# ============================================================================
# TASK 4: Comprehensive Customer Profile
# ============================================================================

print("\n" + "=" * 80)
print("TASK 4: COMPREHENSIVE CUSTOMER PROFILE")
print("=" * 80)

"""
Combine all metrics into a complete customer profile
"""

print("\nCreating comprehensive customer profile...")

# Join all metrics
customer_profile = customer_metrics \
    .join(most_frequent_category, "customer_id", "left") \
    .join(most_frequent_payment, "customer_id", "left")

# Add customer tier based on spending
customer_profile = customer_profile.withColumn(
    "customer_tier",
    when(col("total_spending") > 5000, "Platinum")
    .when(col("total_spending") > 2000, "Gold")
    .when(col("total_spending") > 500, "Silver")
    .otherwise("Bronze")
)

print("\nComprehensive Customer Profile:")
customer_profile.orderBy(col("total_spending").desc()).show(10, truncate=False)

# Save summary statistics
print("\nCustomer Tier Distribution:")
customer_profile.groupBy("customer_tier").agg(
    count("*").alias("customer_count"),
    avg("total_spending").alias("avg_spending"),
    avg("transaction_count").alias("avg_transactions")
).orderBy("customer_tier").show()

# ============================================================================
# TASK 5: Top 10 Customers by Total Spending
# ============================================================================

print("\n" + "=" * 80)
print("TASK 5: TOP 10 CUSTOMERS")
print("=" * 80)

print("\nTop 10 Customers by Total Spending:")
top_customers = customer_profile.orderBy(col("total_spending").desc()).limit(10)
top_customers.show(10, truncate=False)

# Calculate top 10% contribution
total_revenue = df.agg(sum("amount")).collect()[0][0]
top_10_revenue = top_customers.agg(sum("total_spending")).collect()[0][0]
contribution_pct = (top_10_revenue / total_revenue) * 100

print(f"\nRevenue Analysis:")
print(f"  Total Revenue: ${total_revenue:,.2f}")
print(f"  Top 10 Revenue: ${top_10_revenue:,.2f}")
print(f"  Top 10 Contribution: {contribution_pct:.1f}%")

"""
Learning Point: Pareto Principle (80/20 Rule)
Often, ~20% of customers generate ~80% of revenue
This helps identify VIP customers for targeted marketing
"""

# ============================================================================
# TASK 6: Monthly Revenue by Region
# ============================================================================

print("\n" + "=" * 80)
print("TASK 6: MONTHLY REVENUE BY REGION")
print("=" * 80)

"""
Calculate monthly revenue broken down by region
"""

print("\nCalculating monthly revenue by region...")

# Extract year-month from transaction_date
df_with_month = df.withColumn(
    "year_month",
    date_format(to_date(col("transaction_date")), "yyyy-MM")
)

# Aggregate by month and region
monthly_regional_revenue = df_with_month.groupBy("year_month", "region").agg(
    sum("amount").alias("revenue"),
    count("*").alias("transaction_count"),
    countDistinct("customer_id").alias("unique_customers")
).orderBy("year_month", "region")

print("\nMonthly Revenue by Region:")
monthly_regional_revenue.show(20)

# Pivot to see regions as columns
print("\nPivoted View (Regions as Columns):")
pivoted_revenue = df_with_month.groupBy("year_month").pivot("region").agg(
    sum("amount")
).orderBy("year_month")

pivoted_revenue.show(10)

"""
Learning Point: Pivot Operations

pivot() transforms:
From:
  year_month | region | revenue
  2024-01    | North  | 1000
  2024-01    | South  | 1500

To:
  year_month | North | South
  2024-01    | 1000  | 1500

Useful for:
- Creating crosstab reports
- Time series by category
- Comparison matrices
"""

# ============================================================================
# TASK 7: Advanced Aggregations
# ============================================================================

print("\n" + "=" * 80)
print("TASK 7: ADVANCED AGGREGATIONS")
print("=" * 80)

"""
More complex aggregations:
- Collect lists of values
- Approximate statistics
- Conditional aggregations
"""

print("\n1. Collect Lists of Categories per Customer:")
customer_categories = df.groupBy("customer_id").agg(
    collect_set("category").alias("categories_purchased"),
    collect_list("product_id").alias("all_products"),
    countDistinct("category").alias("category_diversity")
).filter(col("category_diversity") >= 3)

customer_categories.show(5, truncate=False)

"""
collect_set vs collect_list:
- collect_set(): Unique values only (no duplicates)
- collect_list(): All values including duplicates

Warning: Can cause OOM if too many values per group!
"""

print("\n2. Percentile Aggregations (Approximate):")
percentile_stats = df.groupBy("region").agg(
    expr("percentile_approx(amount, 0.25)").alias("p25"),
    expr("percentile_approx(amount, 0.50)").alias("p50_median"),
    expr("percentile_approx(amount, 0.75)").alias("p75"),
    expr("percentile_approx(amount, 0.95)").alias("p95")
)

print("\nPercentile Statistics by Region:")
percentile_stats.show()

"""
percentile_approx():
- Fast approximate percentile
- Good for large datasets
- Accuracy: ~99%
- Much faster than exact percentile
"""

print("\n3. Conditional Aggregations:")
conditional_aggs = df.groupBy("region").agg(
    # Count high-value transactions
    sum(when(col("amount") > 500, 1).otherwise(0)).alias("high_value_count"),
    
    # Average of high-value transactions only
    avg(when(col("amount") > 500, col("amount"))).alias("avg_high_value"),
    
    # Percentage of high-value transactions
    (sum(when(col("amount") > 500, 1).otherwise(0)) / count("*") * 100).alias("high_value_pct")
)

print("\nConditional Aggregations by Region:")
conditional_aggs.show()

"""
Conditional Aggregations Pattern:
sum(when(condition, 1).otherwise(0)) → Count matching rows
avg(when(condition, col)) → Average of matching values
"""

# ============================================================================
# TASK 8: Customer Segmentation
# ============================================================================

print("\n" + "=" * 80)
print("TASK 8: CUSTOMER SEGMENTATION (RFM Analysis)")
print("=" * 80)

"""
RFM Analysis:
- Recency: Days since last purchase
- Frequency: Number of purchases
- Monetary: Total spending

Used for customer segmentation in marketing
"""

print("\nCalculating RFM metrics...")

from datetime import datetime

# Get current date (max date in dataset as reference)
max_date = df.agg(max(to_date("transaction_date"))).collect()[0][0]

rfm = df.groupBy("customer_id").agg(
    # Recency: Days since last purchase
    datediff(lit(max_date), max(to_date("transaction_date"))).alias("recency_days"),
    
    # Frequency: Number of transactions
    count("*").alias("frequency"),
    
    # Monetary: Total spending
    sum("amount").alias("monetary")
)

# Score each dimension (1-5)
rfm_scored = rfm.withColumn(
    "recency_score",
    when(col("recency_days") <= 30, 5)
    .when(col("recency_days") <= 60, 4)
    .when(col("recency_days") <= 90, 3)
    .when(col("recency_days") <= 180, 2)
    .otherwise(1)
).withColumn(
    "frequency_score",
    when(col("frequency") >= 20, 5)
    .when(col("frequency") >= 15, 4)
    .when(col("frequency") >= 10, 3)
    .when(col("frequency") >= 5, 2)
    .otherwise(1)
).withColumn(
    "monetary_score",
    when(col("monetary") >= 5000, 5)
    .when(col("monetary") >= 2000, 4)
    .when(col("monetary") >= 1000, 3)
    .when(col("monetary") >= 500, 2)
    .otherwise(1)
)

# Overall RFM score
rfm_final = rfm_scored.withColumn(
    "rfm_score",
    (col("recency_score") + col("frequency_score") + col("monetary_score")) / 3
).withColumn(
    "segment",
    when(col("rfm_score") >= 4.5, "Champions")
    .when(col("rfm_score") >= 4.0, "Loyal")
    .when(col("rfm_score") >= 3.0, "Potential")
    .when(col("rfm_score") >= 2.0, "At Risk")
    .otherwise("Lost")
)

print("\nRFM Customer Segments:")
rfm_final.groupBy("segment").agg(
    count("*").alias("customer_count"),
    avg("monetary").alias("avg_spending"),
    avg("frequency").alias("avg_frequency"),
    avg("recency_days").alias("avg_recency")
).orderBy("segment").show()

print("\nTop Champions:")
rfm_final.filter(col("segment") == "Champions") \
    .orderBy(col("rfm_score").desc()) \
    .show(10)

# ============================================================================
# KEY TAKEAWAYS
# ============================================================================

print("\n" + "=" * 80)
print("KEY TAKEAWAYS")
print("=" * 80)

takeaways = """
1. BASIC AGGREGATIONS:
   ✓ sum(), avg(), count(), min(), max()
   ✓ stddev(), variance() for variability
   ✓ countDistinct() for unique values

2. FINDING MODE (Most Frequent):
   ✓ Group by dimension
   ✓ Count occurrences
   ✓ Use window function to rank
   ✓ Filter rank = 1

3. MULTIPLE AGGREGATIONS:
   ✓ Chain multiple .agg() functions
   ✓ Use alias() for readable column names
   ✓ Join results for comprehensive profiles

4. ADVANCED TECHNIQUES:
   ✓ collect_set/collect_list for arrays
   ✓ percentile_approx for distribution
   ✓ Conditional aggregations with when()
   ✓ pivot() for crosstab reports

5. CUSTOMER ANALYTICS:
   ✓ RFM analysis for segmentation
   ✓ Customer tiers based on spending
   ✓ Pareto analysis (top contributors)
   ✓ Cohort analysis by time period

6. PERFORMANCE TIPS:
   ✓ Use countDistinct sparingly (shuffle)
   ✓ Prefer approx functions for large data
   ✓ Limit collect_list size (OOM risk)
   ✓ Filter before groupBy when possible

7. BUSINESS APPLICATIONS:
   ✓ Customer lifetime value
   ✓ Churn prediction (recency)
   ✓ Targeted marketing (segments)
   ✓ Revenue forecasting (trends)
"""

print(takeaways)

spark.stop()

print("\n" + "=" * 80)
print("EXERCISE COMPLETE!")
print("=" * 80)
