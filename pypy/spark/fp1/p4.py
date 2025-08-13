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
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    consumer = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "src_2") \
        .option("startingOffsets", "earliest") \
        .load()
    
    query = consumer.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()
    
    query.awaitTermination()


if __name__ == "__main__":
    main()
