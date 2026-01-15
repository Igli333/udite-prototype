import os
import sys
from pyspark.sql.functions import (
    col, window, avg, count, to_json, struct, lit, concat, cast
)
from pyspark.sql.types import StringType

from app.common.spark_session import get_spark


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GOLD_TOPIC = os.getenv("GOLD_TOPIC", "processed.data")

SILVER_PATH = os.getenv("SILVER_PATH", "/opt/data/silver/sensors_cleaned")
GOLD_PATH = os.getenv("GOLD_PATH", "/opt/data/gold/sensors_gold")
CHECKPOINT = os.getenv("GOLD_CHECKPOINT", "/opt/checkpoints/gold/sensors_gold")

os.makedirs(GOLD_PATH, exist_ok=True)
os.makedirs(CHECKPOINT, exist_ok=True)

spark = get_spark("gold-silver-to-kafka")

silver_stream = spark.readStream.format("delta").load(SILVER_PATH)

agg = (
    silver_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("topic"),
        col("district"),
    )
    .agg(
        avg(col("value")).alias("avg_value"),
        count(lit(1)).alias("event_count"),
    )
)

gold_delta_query = (
    agg.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT + "_delta")
    .option("path", GOLD_PATH)
    .start()
)

kafka_out = agg.select(
    concat(col("topic"), lit("|"), col("district"))
    .cast(StringType())  # Explicit cast to string
    .alias("key"),
    to_json(struct(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("topic"),
        col("district"),
        col("avg_value"),
        col("event_count"),
    )).alias("value")
)

gold_kafka_query = (
    kafka_out.writeStream
    .format("kafka")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT + "_kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", GOLD_TOPIC)
    .start()
)

# Await termination of both streams 
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Streaming interrupted by user.", file=sys.stderr)
finally:
    gold_delta_query.stop()
    gold_kafka_query.stop()