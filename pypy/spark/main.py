import time
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def tick_tock(func):
    def wrapper(*args, **kwargs):
        tick = time.time()
        result = func(*args, **kwargs)
        tock = time.time()
        print(f"Time taken by {func.__name__}: {round(tock - tick, 2)}")
        return result
    return wrapper

@tick_tock
def create_sample_data(spark):
    transactions = []
    for i in range(1000):
        transactions.append({
            "txn_id": f"txn_{i}",
            "cust_id": f"cust_{random.randint(1, 100)}",
            "txn_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
            "amount": round(random.uniform(10, 10000), 2),
            "category": random.choice(["Electronics", "Clothing", "Books", "Home Appliances", "Furniture"]),
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "UPI", "Wallet", "COD"])
        })

    return spark.createDataFrame(transactions)


def main():
    spark = SparkSession.builder \
        .appName("practice") \
        .master("local[2]")  \
        .getOrCreate()
    
    df = create_sample_data(spark)
# basic transformations 
    df_f = df.select("txn_id", "amount", "category", "payment_method") \
        .filter(F.col("amount") > 100) \
        .withColumn("amount_category",
        F.when(F.col("amount") > 5000, "High") \
            .when(F.col("amount") < 2000, "Low") \
            .otherwise("Medium")) \
        .sort(F.col("amount").desc())
    
# Narrow Transformation
    df_with_array = df.withColumn("tags", F.split(F.col("category"), ""))
    df_with_array.show()
    df_exploded = df_with_array.withColumn("tag", F.explode("tags")) \
        .filter(F.col("tag") != " ")
    # df_exploded.show()
    print(f"After flatMap/explode: {df_exploded.rdd.getNumPartitions()} partitions")

# Wide Transformation
    df_cust = df.select("cust_id").distinct()
    df_j_cust = df.join(df_cust, df.cust_id == df_cust.cust_id, "inner")
    # df_j_cust.show()
    print(f"After join: {df_j_cust.rdd.getNumPartitions()} partitions")
    # df_j_cust.explain(True)

    df_g_cat = df.groupBy("category") \
        .agg(F.round(F.sum("amount"), 2).alias("total_amount"),
        # F.collect_list("cust_id").alias("customers"),
        F.count("txn_id").alias("txn_count")
        )
    # df_g_cat.show()

# TODO: Calculate for each customer:
# 1. Total spending
# 2. Average transaction amount
# 3. Number of transactions
# 4. Most frequent category purchased
# 5. Most frequent payment method

    # Option 1: Using collect_list and custom function with UDF
    from pyspark.sql.types import StringType
    
    def find_most_frequent(values_list):
        if not values_list:
            return None
        freq_count = {}
        for value in values_list:
            freq_count[value] = freq_count.get(value, 0) + 1
        return max(freq_count, key=freq_count.get)
    
    # Register UDF
    most_frequent_udf = F.udf(find_most_frequent, StringType())
    
    # Calculate aggregations per customer
    df3 = df.groupBy("cust_id") \
        .agg(F.round(F.sum("amount"), 2).alias("total_spending"),
            F.round(F.avg("amount"), 2).alias("avg_transaction"),
            F.count("txn_id").alias("txn_count"),
            F.mode("payment_method").alias("most_frequent_payment_method"),
            F.collect_list("category").alias("categories")) \
        .withColumn("most_frequent_category", most_frequent_udf(F.col("categories"))) \
        .drop("categories")
    
    df3.show()

# TODO: Find the top 10 customers by total spending
    df3_1 = df3.sort(F.col("total_spending").desc()).limit(10)
    df3_1.show()

# TODO: Calculate monthly revenue by category
    df3_2 = df.withColumn("year_month", F.date_format(F.col("txn_date"), "yyyy-MM")) \
        .groupBy("year_month", "category") \
        .agg(F.round(F.sum(F.col("amount")), 2).alias("monthly_revenue"),
            F.count("txn_id").alias("txn_count" )) \
        .orderBy("year_month")
    df3_2.show()






    spark.stop()

if __name__ == "__main__":
    main()
