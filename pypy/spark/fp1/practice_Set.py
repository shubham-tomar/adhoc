# PySpark Batch Processing Practice Problems

## Dataset Preparation

# First, let's create sample datasets that we'll use throughout the exercises:


import random
from datetime import datetime, timedelta

builtin_round = round

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("PySpark Practice") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Generate sample data
def create_sample_data():
    # E-commerce transactions
    transactions = []
    for i in range(10000):
        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": f"CUST{random.randint(1, 1000):04d}",
            "product_id": f"PROD{random.randint(1, 100):03d}",
            "amount": builtin_round(random.uniform(10, 1000), 2),
            "quantity": random.randint(1, 5),
            "transaction_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            "category": random.choice(["Electronics", "Clothing", "Books", "Home", "Sports"]),
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Cash"]),
            "region": random.choice(["North", "South", "East", "West"])
        })
    
    # Save as different formats for practice
    import pandas as pd
    import json
    import csv
    
    df = pd.DataFrame(transactions)
    
    # Save as CSV
    df.to_csv("transactions.csv", index=False)
    
    # Save as JSON (records format)
    df.to_json("transactions.json", orient="records", lines=True)
    
    # Save as Parquet
    df.to_parquet("transactions.parquet")
    
    # Create a malformed CSV for error handling practice
    with open("malformed_transactions.csv", "w") as f:
        f.write("transaction_id,customer_id,amount,extra_field\n")
        f.write("TXN001,CUST001,100.50,extra\n")
        f.write("TXN002,CUST002,not_a_number,extra\n")
        f.write("TXN003,CUST003,,extra\n")
    
    print("Sample data created successfully!")

# Run this once to create your practice data
create_sample_data()


## Problem Set 1: Reading Different Formats

### Problem 1.1: Basic File Reading
# Read the same transaction data from CSV, JSON, and Parquet formats. Compare the reading time and examine the inferred schemas.


# Your solution here
# TODO: Read transactions from all three formats
# TODO: Print schema for each
# TODO: Use spark.time() to measure read time
# TODO: Count records in each DataFrame


#Expected Skills#: Understanding different file formats, schema inference, performance characteristics

### Problem 1.2: Schema Enforcement
# Define a strict schema and read the CSV file with it. Handle the malformed CSV file with proper error handling.


# Define the schema
transaction_schema = StructType([
    # TODO: Complete the schema definition
])

# Your solution here
# TODO: Read CSV with enforced schema
# TODO: Read malformed CSV with PERMISSIVE, DROPMALFORMED, and FAILFAST modes
# TODO: Compare the results


#Expected Skills#: Schema definition, error handling modes, data quality management

### Problem 1.3: Advanced Reading Options
# Read a subset of Parquet columns (column pruning) and apply predicate pushdown filters.


# Your solution here
# TODO: Read only transaction_id, amount, and category columns from Parquet
# TODO: Apply filter for amount > 500 during the read operation
# TODO: Verify the physical plan to confirm predicate pushdown


#Expected Skills#: Column pruning, predicate pushdown, query optimization

## Problem Set 2: Transformations vs Actions

### Problem 2.1: Lazy Evaluation Understanding
# Create a series of transformations and observe when actual computation happens.


# Your solution here
df = spark.read.csv("transactions.csv", header=True, inferSchema=True)

# TODO: Chain these transformations (none should trigger computation):
# 1. Filter transactions > $100
# 2. Select only customer_id, amount, category
# 3. Add a new column 'amount_category' (High if > 500, Medium if > 100, Low otherwise)
# 4. Sort by amount descending

# TODO: Now trigger computation with different actions:
# 1. count()
# 2. show(5)
# 3. collect() - be careful with large datasets!
# 4. first()
# 5. take(10)

# TODO: Use df.explain() to see the execution plan


#Expected Skills#: Lazy evaluation, transformation vs action distinction, execution planning

### Problem 2.2: Transformation Types
# Implement both narrow and wide transformations and understand their impact.


# Your solution here
# Narrow transformations (no shuffle):
# TODO: filter, map, flatMap operations

# Wide transformations (require shuffle):
# TODO: groupBy, join, distinct operations

# TODO: Check number of partitions before and after each operation
# TODO: Use spark.conf.set("spark.sql.shuffle.partitions", "10") to control shuffle partitions


#Expected Skills#: Narrow vs wide transformations, shuffle operations, partition management



## Problem Set 3: Window Functions and GroupBy

### Problem 3.1: Customer Analytics with GroupBy
# Calculate various customer metrics using groupBy operations.


# Your solution here
df = spark.read.parquet("transactions.parquet")

# TODO: Calculate for each customer:
# 1. Total spending
# 2. Average transaction amount
# 3. Number of transactions
# 4. Most frequent category purchased
# 5. Most frequent payment method

# TODO: Find the top 10 customers by total spending

# TODO: Calculate monthly revenue by region


#Expected Skills#: GroupBy, aggregation functions, multiple aggregations

### Problem 3.2: Window Functions - Running Totals and Rankings
# Use window functions for complex analytics.


# Your solution here
# TODO: For each customer, calculate:
# 1. Running total of spending over time
# 2. Rank of each transaction by amount (within customer)
# 3. Difference from previous transaction amount
# 4. Average of last 3 transactions (moving average)

# TODO: Find the top 3 transactions for each category

# TODO: Calculate percentile rank of each transaction amount within its region


#Expected Skills#: Window specifications, ranking functions, analytical functions

### Problem 3.3: Complex Window Operations
# Implement session windows and time-based analytics.


# Your solution here
# TODO: Identify "shopping sessions" - transactions by same customer within 7 days
# TODO: Calculate session-level metrics (total amount, number of items, session duration)
# TODO: Find customers with increasing spending trend (compare first half vs second half of their history)


#Expected Skills#: Complex window logic, time-based windows, trend analysis



## Problem Set 4: Repartition and Coalesce

### Problem 4.1: Understanding Partitioning
# Explore how data is distributed across partitions.


# Your solution here
df = spark.read.csv("transactions.csv", header=True, inferSchema=True)

# TODO: Check initial number of partitions
# TODO: Check partition sizes using df.rdd.glom().map(len).collect()

# TODO: Repartition to 20 partitions and observe distribution
# TODO: Repartition by 'region' column (4 unique values) - what happens?
# TODO: Repartition to 2 partitions using coalesce vs repartition - compare performance


#Expected Skills#: Partition management, repartition vs coalesce, data distribution

### Problem 4.2: Optimizing for Joins
# Prepare data optimally for join operations.


# Create a second dataset - customer details
customers_data = [(f"CUST{i:04d}", f"Customer_{i}", random.choice(["Premium", "Regular", "Basic"])) 
                   for i in range(1, 1001)]
customers_df = spark.createDataFrame(customers_data, ["customer_id", "customer_name", "tier"])

# Your solution here
# TODO: Join transactions with customers - observe shuffle
# TODO: Broadcast the smaller DataFrame and compare performance
# TODO: Pre-partition both DataFrames by join key and compare
# TODO: Use sort-merge join vs broadcast join - compare execution plans


#Expected Skills#: Join optimization, broadcast joins, partition strategies

### Problem 4.3: Output Partitioning Strategy
# Control output file partitioning for different scenarios.


# Your solution here
# TODO: Scenario 1: Write one file per region
# TODO: Scenario 2: Write exactly 5 evenly-sized files
# TODO: Scenario 3: Write files no larger than 128MB each
# TODO: Scenario 4: Partition by date and region (nested partitioning)


#Expected Skills#: Output partitioning, file size control, partitioned tables



## Problem Set 5: Writing Different Formats

### Problem 5.1: Format Comparison
# Write the same data in different formats and compare.


# Your solution here
result_df = df.filter(col("amount") > 100).groupBy("category").agg(
    sum("amount").alias("total_amount"),
    count("*").alias("transaction_count")
)

# TODO: Write as CSV with header
# TODO: Write as JSON with different save modes (append, overwrite, ignore, error)
# TODO: Write as Parquet with compression (snappy, gzip, lz4)
# TODO: Write as ORC
# TODO: Compare file sizes and read performance


#Expected Skills#: Output formats, compression options, save modes

### Problem 5.2: Partitioned Tables
# Write partitioned tables and understand partition pruning.


# Your solution here
# TODO: Write partitioned by year/month/day hierarchy
# TODO: Write partitioned by category
# TODO: Read back with partition filters and verify partition pruning
# TODO: Handle partition column in output (include vs exclude)


#Expected Skills#: Partitioned tables, partition pruning, directory structure

### Problem 5.3: Advanced Output Control
# Implement bucketing and optimize for downstream queries.


# Your solution here
# TODO: Write bucketed table by customer_id (10 buckets)
# TODO: Sort within buckets by transaction_date
# TODO: Write with maxRecordsPerFile option
# TODO: Implement dynamic partition overwrite


#Expected Skills#: Bucketing, sorting, output optimization



## Problem Set 6: Performance Optimization and Configs

### Problem 6.1: Memory Management
# Optimize Spark memory settings for different workloads.


# Create a memory-intensive operation
spark.conf.set("spark.sql.adaptive.enabled", "false")  # Disable for controlled testing

# Your solution here
# TODO: Create a large DataFrame with cross join (careful - use small samples)
# TODO: Monitor memory usage with different settings:
#       - spark.executor.memory
#       - spark.executor.memoryOverhead  
#       - spark.memory.fraction
#       - spark.memory.storageFraction

# TODO: Trigger OOM error and fix with proper memory settings
# TODO: Use cache() and persist() with different storage levels


#Expected Skills#: Memory configuration, caching strategies, troubleshooting OOM

### Problem 6.2: Shuffle Optimization
# Optimize shuffle-heavy operations.


# Your solution here
# TODO: Perform a large groupBy aggregation
# TODO: Tune these shuffle configs and measure impact:
#       - spark.sql.shuffle.partitions (try 50, 200, 500)
#       - spark.sql.adaptive.skewJoin.enabled
#       - spark.sql.adaptive.coalescePartitions.enabled

# TODO: Implement salting for skewed joins
# TODO: Use aggregate functions that support partial aggregation


#Expected Skills#: Shuffle tuning, handling data skew, adaptive query execution

### Problem 6.3: Catalyst Optimizer and Tungsten
# Understand and leverage Spark's optimization engines.


# Your solution here
# TODO: Write inefficient query with multiple filters and see optimizer combine them
# TODO: Compare performance with/without whole-stage code generation:
#       spark.sql.codegen.wholeStage = true/false

# TODO: Use DataFrame API vs RDD API for same operation - compare performance
# TODO: Force broadcast join with hint: df.hint("broadcast")
# TODO: Examine physical plans for optimization verification


#Expected Skills#: Catalyst optimizer, Tungsten engine, query optimization



## Problem Set 7: Real-World Scenarios

### Problem 7.1: ETL Pipeline
# Build a complete ETL pipeline with error handling.


# Your solution here
# TODO: Extract - Read from multiple sources (CSV, JSON, Parquet)
# TODO: Transform - 
#       - Clean nulls and invalid values
#       - Standardize date formats
#       - Derive new columns (customer lifetime value, transaction velocity)
#       - Handle late arriving data
# TODO: Load - 
#       - Write to partitioned Parquet table
#       - Generate summary statistics
#       - Create data quality report


#Expected Skills#: Complete ETL, error handling, data quality

### Problem 7.2: Incremental Processing
# Implement incremental data processing pattern.


# Your solution here
# TODO: Read existing processed data
# TODO: Identify new records (watermarking)
# TODO: Process only new records
# TODO: Merge with existing data (handling updates and deletes)
# TODO: Write back maintaining partitioning


#Expected Skills#: Incremental processing, merge operations, state management

### Problem 7.3: Performance Troubleshooting
# Debug and optimize a slow Spark job.


# Intentionally inefficient code
def slow_operation():
    df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
    
    # Multiple passes over data
    high_value = df.filter(col("amount") > 500).count()
    low_value = df.filter(col("amount") <= 100).count()
    medium_value = df.filter((col("amount") > 100) & (col("amount") <= 500)).count()
    
    # Inefficient join
    df1 = df.select("customer_id", "amount")
    df2 = df.select("customer_id", "category")
    result = df1.join(df2, "customer_id")
    
    # Collecting large data
    all_data = result.collect()
    
    return all_data

# Your solution here
# TODO: Identify performance issues
# TODO: Rewrite efficiently
# TODO: Use Spark UI to verify improvements
# TODO: Document optimization techniques used


#Expected Skills#: Performance debugging, Spark UI interpretation, optimization techniques



## Problem Set 8: Advanced Challenge Problems

### Problem 8.1: Data Skew Resolution
# Handle severely skewed data in joins and aggregations.


# Create skewed dataset
skewed_data = []
# 90% of transactions from one customer
for i in range(9000):
    skewed_data.append((f"TXN{i}", "CUST0001", random.uniform(10, 100)))
for i in range(9000, 10000):
    skewed_data.append((f"TXN{i}", f"CUST{random.randint(2, 1000):04d}", random.uniform(10, 100)))

skewed_df = spark.createDataFrame(skewed_data, ["txn_id", "customer_id", "amount"])

# Your solution here
# TODO: Identify skew using statistics
# TODO: Implement salting technique
# TODO: Use adaptive query execution features
# TODO: Compare performance before/after optimization


### Problem 8.2: Complex Business Logic
# Implement session analysis with complex business rules.


# Your solution here
# TODO: Define shopping sessions (gap > 30 minutes = new session)
# TODO: Calculate:
#       - Session duration
#       - Items per session
#       - Session conversion (session with purchase > $50)
#       - User journey (sequence of categories in session)
# TODO: Find patterns in successful vs abandoned sessions


### Problem 8.3: Memory and Disk Optimization
# Process data larger than available memory.


# Your solution here
# TODO: Generate large dataset (>memory)
# TODO: Implement external sort
# TODO: Use disk-based caching
# TODO: Optimize with partition pruning and projection pushdown
# TODO: Monitor spill to disk and optimize




## Solutions Verification Checklist

# For each problem, verify your solution addresses:

#Correctness#: Does it produce the expected output?
#Performance#: Is it optimized for the given scenario?
#Scalability#: Will it work with 100x more data?
#Maintainability#: Is the code clean and documented?
#Error Handling#: Does it handle edge cases and bad data?

## Performance Monitoring Commands
# Use these throughout your practice:
# Check partition information
df.rdd.getNumPartitions()
df.rdd.glom().map(len).collect()

# Execution plan analysis
df.explain(True)  # All plans
df.explain("formatted")  # Formatted physical plan
df.explain("cost")  # With cost information

# Cache monitoring
spark.catalog.isCached("table_name")
spark.catalog.clearCache()

# Configuration check
spark.conf.get("spark.sql.shuffle.partitions")
spark.sparkContext.getConf().getAll()

# Memory usage
spark.sparkContext.statusTracker().getExecutorInfos()


## Tips for Practice

 #Start Small#: Begin with small datasets (1000 rows) to understand behavior
 #Scale Gradually#: Increase data size to observe performance changes
 #Use Spark UI#: Always check localhost:4040 for job details
 #Compare Approaches#: Try multiple solutions and compare performance
 #Document Learnings#: Keep notes on what works and why

## Additional Resources

# - Spark SQL Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
# - Tuning Guide: https://spark.apache.org/docs/latest/tuning.html
# - Best Practices: https://spark.apache.org/docs/latest/sql-performance-tuning.html

# Remember: The key to mastering PySpark is understanding not just what works, but why it works and when to use each technique.