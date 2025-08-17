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
        print(f"Time taken by {func.__name__}: {tock - tick}")
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
    df.show()

    df_f = df.select("txn_id", "amount", "category", "payment_method") \
        .filter(F.col("amount") > 100)
    
    df_f.show()
    
    spark.stop()

if __name__ == "__main__":
    main()
