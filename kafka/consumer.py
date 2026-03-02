from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Create Spark session
spark = (
    SparkSession.builder
    .appName("KafkaSparkConsumer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read stream from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "transactions_csv")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Kafka value (bytes) → string
messages_df = kafka_df.select(
    col("value").cast(StringType()).alias("message")
)

# Processing (no transformation yet)
processed_df = messages_df

# Write stream to console every 5 seconds
query = (
    processed_df.writeStream
    .format("console")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
