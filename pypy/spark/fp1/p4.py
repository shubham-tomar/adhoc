from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from argparse import ArgumentParser
import json

def parse_args():
    p = ArgumentParser()
    p.add_argument("--dummy", default="dummy", help="Dummy argument (default: dummy)")
    return p.parse_args()

def main():
    args = parse_args()
    spark = SparkSession.builder \
        .appName("p4") \
        .master("local[2]") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.stateStore.backend", "rocksdb") \
        .config("spark.sql.streaming.kafka.maxOffsetsPerTrigger", "10000") \
        .getOrCreate()
    
    consumer = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "src_2") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # |{"active":false,"clicks":712,"created_at":1755097410,"id":"iRA420Sh3y","name":"eVVfohljYs","status":"SUCCESS"}
    schema = StructType([
        StructField("active", BooleanType()),
        StructField("clicks", IntegerType()),
        StructField("created_at", IntegerType()),
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("status", StringType())
    ])
    
    # parsed = consumer.selectExpr("CAST(value AS STRING)")
    parsed = consumer.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    parsed.printSchema()
    
    query = parsed.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()
    
    query.awaitTermination()


if __name__ == "__main__":
    main()
