from pyspark.sql import SparkSession

# start spark session
spark = SparkSession.builder \
    .appName("fp1") \
    .master("local[2]") \
    .getOrCreate()

df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.show()

df_f = df.filter("age > 30")
df_f.show()

df_g = df_f.groupBy("city").count()
df_g.show()

df_g.explain(True)

input("Open Spark UI at http://localhost:4040 and press Enter to end...")