from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from elasticsearch import Elasticsearch
import json

# Initialize Spark Session with Kafka and Elasticsearch packages
spark = SparkSession.builder \
    .appName("StockPriceStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9093"
KAFKA_TOPIC = "stock-prices"

# Elasticsearch configuration
ES_HOST = "elasticsearch"
ES_PORT = "9200"
ES_INDEX = "stock-prices"

# Define schema for stock data
stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
parsed_df = df.select(
    col("key").cast("string").alias("key"),
    from_json(col("value").cast("string"), stock_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
)

# Extract fields
# Extract fields - convert to proper timestamp format
final_df = parsed_df.select(
    col("data.symbol").alias("symbol"),
    col("data.price").alias("price"),
    (col("data.timestamp") * 1000).cast("long").alias("timestamp"),
    col("data.timestamp").alias("timestamp_unix"),
    (unix_timestamp(current_timestamp()) * 1000).cast("long").alias("processed_time")
)

# Add additional derived fields using timestamp_unix
enriched_df = final_df \
    .withColumn("date", to_date(from_unixtime(col("timestamp_unix")))) \
    .withColumn("hour", hour(from_unixtime(col("timestamp_unix")))) \
    .withColumn("minute", minute(from_unixtime(col("timestamp_unix"))))

# Write to Elasticsearch
def write_to_elasticsearch(batch_df, batch_id):
    """Custom function to write each batch to Elasticsearch"""
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    # Write to Elasticsearch using DataFrame write
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", ES_PORT) \
        .option("es.resource", ES_INDEX) \
        .option("es.mapping.id", "timestamp_unix") \
        .option("es.write.operation", "upsert") \
        .option("es.nodes.wan.only", "true") \
        .mode("append") \
        .save()
    
    print(f"Batch {batch_id} written to Elasticsearch successfully")

# Start streaming query with foreachBatch
query = enriched_df \
    .writeStream \
    .foreachBatch(write_to_elasticsearch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/stock-prices") \
    .start()

print("=" * 70)
print("Spark Streaming Application Started")
print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
print(f"Writing to Elasticsearch: {ES_HOST}:{ES_PORT}/{ES_INDEX}")
print("=" * 70)

# Also print to console for monitoring
console_query = enriched_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for termination
query.awaitTermination()
console_query.awaitTermination()