from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ph2-nyc-taxi") \
    .master("local[2]") \
    .config("spark.sql.files.minPartitionNum", "2") \
    .getOrCreate()

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

df = spark.read.csv("nyc-taxi.csv", header = True, schema = schema)
print(f"Initial partitions: {df.rdd.getNumPartitions()}")
print(df.count())
# print(f"After count partitions: {df.rdd.getNumPartitions()}")
df_f = df.filter(df.passenger_count > 0) \
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "tip_amount", "total_amount", "PULocationID", "DOLocationID")
# print(f"After count partitions: {df_f.rdd.getNumPartitions()}")
df.explain(True)

print(df_f.count())

df_1 = df_f.groupBy("PULocationID").avg("fare_amount")

df_1.show()

df_1.explain(True)

# Rank trips by fare amount for each pickup location:

window_spec = Window.partitionBy("PULocationID").orderBy(desc("fare_amount"))
df_w1 = df_f.withColumn("trip_rank", dense_rank().over(window_spec))

df_zone = spark.read.csv("nyc-zone.csv", header = True)

df_join = df_f.join(broadcast(df_zone), df_f.PULocationID == df_zone.LocationID)

print(df_join.select("Zone", "fare_amount", "trip_distance").count())

# 1244687

input("Press Enter to Exit...")