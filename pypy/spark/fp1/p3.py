from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from argparse import ArgumentParser

def parse_args():
    parser = ArgumentParser()
    # parser.add_argument("--input", required=True, type=str)
    return parser.parse_args()

args = parse_args()

spark = SparkSession.builder \
    .appName("p3") \
    .master("local[2]") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .config("spark.sql.tungsten.enabled", "true") \
    .getOrCreate()

# df = spark.range(0, 5_000_000).toDF("id").withColumn("k", (F.col("id")%100))

# df1 = df.filter(df.id > 10)
# df1.count()

# spark.conf.set("spark.sql.shuffle.partitions", "64")
# df2 = df.groupBy("k").count()
# df2.collect()

# Create sample data for shuffle examples
orders = spark.createDataFrame([
    (1, 100, "2024-01-01", "US"),
    (2, 200, "2024-01-01", "UK"),
    (3, 150, "2024-01-02", "US"),
    (4, 300, "2024-01-02", "UK")
], ["order_id", "amount", "date", "country"])

customers = spark.createDataFrame([
    (100, "Alice", "US"),
    (200, "Bob", "UK"),
    (150, "Charlie", "US"),
    (300, "David", "UK")
], ["customer_id", "name", "country"])

# BAD: Multiple shuffles
result_bad = orders \
    .groupBy("country").agg(F.sum("amount").alias("total")) \
    .orderBy("total") \
    .limit(10)
# This causes 2 shuffles: groupBy and orderBy
result_bad.explain("formatted")

# Option 2: Use window functions to avoid multiple shuffles
from pyspark.sql.window import Window

window = Window.partitionBy("country")
result_good = orders \
    .withColumn("country_total", F.sum("amount").over(window)) \
    .select("country", "country_total").distinct() \
    .orderBy("country_total")
# Only 1 shuffle for the window operation
result_good.explain("formatted")

# Option 3: Pre-partition data to avoid shuffles
orders_partitioned = orders.repartition("country")
orders_partitioned.write.partitionBy("country").mode("overwrite").parquet("/tmp/orders_by_country")

# Future operations on same partition key don't shuffle
df = spark.read.parquet("/tmp/orders_by_country")
result = df.groupBy("country").agg(F.sum("amount"))  # No shuffle if reading partitioned data

result.write.format("parquet") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("/tmp/result")

# spark.stop()

input("Press any key to exit...")
