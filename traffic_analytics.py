from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour, avg, sum as spark_sum, count, when
from pyspark.sql.types import (
    StructType, StructField, TimestampType, StringType,
    IntegerType, FloatType
)

# -----------------------------
# Step 0: Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("TrafficAnalytics") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/traffic_db") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Step 1: Define Schema
# -----------------------------
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("location", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed_kmh", FloatType()),
    StructField("congestion_level", StringType()),
    StructField("road_type", StringType()),
    StructField("direction", StringType())
])

# -----------------------------
# Step 2: Read from Kafka
# -----------------------------
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .option("startingOffsets", "latest") \
    .load()

traffic_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# -----------------------------
# Step 3: Add Numeric Congestion Score
# -----------------------------
traffic_df = traffic_df.withColumn(
    "congestion_score",
    when(col("congestion_level") == "Low", 0.3)
    .when(col("congestion_level") == "Medium", 0.6)
    .otherwise(1.0)
)

# -----------------------------
# Step 4: Traffic Flow Over Time
# -----------------------------
traffic_flow = traffic_df \
    .withColumn("hour", hour(col("timestamp"))) \
    .groupBy("location", "hour") \
    .agg(spark_sum("vehicle_count").alias("total_vehicle_count"))

q1 = traffic_flow.writeStream \
    .format("mongodb") \
    .option("connection.uri", "mongodb://localhost:27017") \
    .option("database", "traffic_db") \
    .option("collection", "traffic_flow") \
    .outputMode("complete") \
    .option("checkpointLocation", "file:///tmp/checkpoint/traffic_flow") \
    .trigger(processingTime="5 seconds") \
    .start()

# -----------------------------
# Step 5: Speed Analysis by Time
# -----------------------------
speed_analysis = traffic_df \
    .withColumn("hour", hour(col("timestamp"))) \
    .groupBy("location", "hour") \
    .agg(avg("avg_speed_kmh").alias("avg_speed"))

q2 = speed_analysis.writeStream \
    .format("mongodb") \
    .option("connection.uri", "mongodb://localhost:27017") \
    .option("database", "traffic_db") \
    .option("collection", "speed_analysis") \
    .outputMode("complete") \
    .option("checkpointLocation", "file:///tmp/checkpoint/speed_analysis") \
    .trigger(processingTime="5 seconds") \
    .start()

# -----------------------------
# Step 6: Vehicle Count by Location
# -----------------------------
vehicle_count_location = traffic_df \
    .groupBy("location") \
    .agg(spark_sum("vehicle_count").alias("total_vehicle_count"))

q3 = vehicle_count_location.writeStream \
    .format("mongodb") \
    .option("connection.uri", "mongodb://localhost:27017") \
    .option("database", "traffic_db") \
    .option("collection", "vehicle_count_location") \
    .outputMode("complete") \
    .option("checkpointLocation", "file:///tmp/checkpoint/vehicle_count_location") \
    .trigger(processingTime="5 seconds") \
    .start()

# -----------------------------
# Step 7: Congestion Distribution
# -----------------------------
congestion_dist = traffic_df \
    .groupBy("congestion_level") \
    .agg(count("*").alias("count"))

q4 = congestion_dist.writeStream \
    .format("mongodb") \
    .option("connection.uri", "mongodb://localhost:27017") \
    .option("database", "traffic_db") \
    .option("collection", "congestion_distribution") \
    .outputMode("complete") \
    .option("checkpointLocation", "file:///tmp/checkpoint/congestion_distribution") \
    .trigger(processingTime="5 seconds") \
    .start()

# -----------------------------
# Step 8: Keep Streaming
# -----------------------------
print("All streaming queries started. Waiting for termination...")
spark.streams.awaitAnyTermination()
