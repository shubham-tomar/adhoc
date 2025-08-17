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
    df_exploded.show()
    print(f"After flatMap/explode: {df_exploded.rdd.getNumPartitions()} partitions")

# Wide Transformation
    df_cust = df.select("cust_id").distinct()
    df_j_cust = df.join(df_cust, df.cust_id == df_cust.cust_id, "inner")
    df_j_cust.show()
    print(f"After join: {df_j_cust.rdd.getNumPartitions()} partitions")
    # df_j_cust.explain(True)

    df_g_cat = df.groupBy("category") \
        .agg(F.round(F.sum("amount"), 2).alias("total_amount"),
        # F.collect_list("cust_id").alias("customers"),
        F.count("txn_id").alias("txn_count")
        )
    df_g_cat.show()
    df_g_cat.explain(True)
    
    spark.stop()

if __name__ == "__main__":
    main()
