from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, udf, to_json, named_struct, from_json, from_unixtime, explode
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.jars.repositories", "https://repo1.maven.org/maven2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for incoming Kafka data
schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji_type", StringType()) \
    .add("timestamp", StringType())  # Ensure this is Unix time (seconds)

# Read from Kafka
emoji_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Convert the Unix timestamp to TimestampType
emoji_stream_with_timestamp = emoji_stream \
    .withColumn("timestamp", from_unixtime(col("timestamp").cast("double")).cast(TimestampType()))

# Aggregate emojis over 2-second windows, counting the occurrences of each emoji
emoji_counts = emoji_stream_with_timestamp \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(window(col("timestamp"), "2 seconds"), col("emoji_type")) \
    .agg(count("*").alias("count"))

# Filter for counts greater than 200
filtered_emoji_counts = emoji_counts \
    .filter(col("count") > 220)

query_debug = filtered_emoji_counts \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()


# Scale the count for each emoji (if needed)
scaled_emoji_counts = filtered_emoji_counts \
    .withColumn("scaled_count", col("count"))

# Define UDF to generate repeated emojis
def generate_repeated_emojis(emoji_type, scaled_count):
    if scaled_count > 0:
        return [emoji_type]
    return []

generate_repeated_emojis_udf = udf(generate_repeated_emojis, ArrayType(StringType()))

# Apply the UDF
repeated_emoji_counts = scaled_emoji_counts \
    .withColumn("repeated_emojis", generate_repeated_emojis_udf(col("emoji_type"), col("scaled_count")))

# Flatten the repeated emojis and include the count
flattened_emoji_counts = repeated_emoji_counts \
    .selectExpr("explode(repeated_emojis) as emoji_type", "window.start as window_start", "window.end as window_end", "scaled_count as count")

# Send the final repeated emoji counts to the main publisher, including the count
query_main_publisher = flattened_emoji_counts \
    .selectExpr(
        "CAST(emoji_type AS STRING) AS key",
        """to_json(named_struct(
            'emoji_type', emoji_type,
            'count', count,
            'window_start', window_start,
            'window_end', window_end
        )) AS value"""
    ) \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "main_publisher_topic") \
    .option("checkpointLocation", "/tmp/checkpoints/emoji_aggregator_main_publisher") \
    .trigger(processingTime="2 seconds") \
    .start()
query_debug.awaitTermination()
query_main_publisher.awaitTermination()

