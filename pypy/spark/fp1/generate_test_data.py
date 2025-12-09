"""
Quick script to generate test data files for Spark practice problems
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import random
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("Generate Test Data") \
    .config("spark.sql.shuffle.partitions", "10") \
    .master("local[*]") \
    .getOrCreate()

print("Generating test data...")

transactions = []
for i in range(10000):
    transactions.append((
        f"TXN{i:05d}",
        f"CUST{random.randint(1, 1000):04d}",
        f"PROD{random.randint(1, 100):03d}",
        round(random.uniform(10, 1000), 2),
        random.randint(1, 5),
        (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        random.choice(["Electronics", "Clothing", "Books", "Home", "Sports"]),
        random.choice(["Credit Card", "Debit Card", "PayPal", "Cash"]),
        random.choice(["North", "South", "East", "West"])
    ))

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("transaction_date", StringType(), False),
    StructField("category", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("region", StringType(), False)
])

df = spark.createDataFrame(transactions, schema)

print(f"Generated {df.count()} transactions")
print("\nSaving to files...")

df.write.mode("overwrite").csv("transactions.csv", header=True)
print("✓ Created transactions.csv")

df.write.mode("overwrite").json("transactions.json")
print("✓ Created transactions.json")

df.write.mode("overwrite").parquet("transactions.parquet")
print("✓ Created transactions.parquet")

with open("malformed_transactions.csv", "w") as f:
    f.write("transaction_id,customer_id,amount,extra_field\n")
    f.write("TXN001,CUST001,100.50,extra\n")
    f.write("TXN002,CUST002,not_a_number,extra\n")
    f.write("TXN003,CUST003,,extra\n")
print("✓ Created malformed_transactions.csv")

print("\n✅ Test data generation complete!")
print("\nYou can now run the solution scripts.")

spark.stop()
