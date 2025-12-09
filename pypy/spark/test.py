from curses import window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Problem 3.1 - GroupBy Analytics") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

print("=" * 80)
print("PROBLEM 3.1: CUSTOMER ANALYTICS WITH GROUPBY")
print("=" * 80)

# Read data
df = spark.read.parquet("fp1/transactions.parquet")
df = df.withColumn("ts", F.to_timestamp(F.col("transaction_date")))
df = df.withColumn("transaction_date", F.to_date(F.col("transaction_date")))

print(f"\nDataset Overview:")
print(f"  Total transactions: {df.count()}")
print(f"  Unique customers: {df.select('customer_id').distinct().count()}")

df.show(5)

# Your solution here
# TODO: Identify "shopping sessions" - transactions by same customer within 7 days
window_1 = Window.partitionBy("customer_id").orderBy("ts")
df1 = df.withColumn("days_diff", F.datediff(F.col("transaction_date"), F.lag(F.col("transaction_date"), 1).over(window_1))) \
    .withColumn(
        "is_new_session",
        F.when(
            F.col("days_diff") > 70, 1
        ).otherwise(0)
    )

df1.show(5)

# TODO: Calculate session-level metrics (total amount, number of items, session duration)
# TODO: Find customers with increasing spending trend (compare first half vs second half of their history)