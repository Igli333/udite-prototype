import os
import sys

from pyspark.sql.functions import (
    col,
    window,
    avg,
    count,
    to_json,
    struct,
    lit,
    concat,
    date_format,
    to_utc_timestamp,
)
from pyspark.sql.types import StringType

from app.common.spark_session import get_spark


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Kafka topics
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "processed.data")  # raw from silver
BUSINESS_TOPIC = os.getenv("BUSINESS_TOPIC", "business.data")     # agg from gold

# Delta paths
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/data/silver/sensors_cleaned")
GOLD_PATH = os.getenv("GOLD_PATH", "/opt/data/gold/sensors_gold")

# Checkpoints
CHECKPOINT_BASE = os.getenv("GOLD_CHECKPOINT", "/opt/checkpoints/gold/sensors_gold")
CP_GOLD_DELTA = CHECKPOINT_BASE + "_delta"
CP_SILVER_KAFKA = CHECKPOINT_BASE + "_silver_kafka"
CP_GOLD_KAFKA = CHECKPOINT_BASE + "_gold_kafka"

os.makedirs(GOLD_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_BASE, exist_ok=True)

spark = get_spark("gold-silver-to-kafka")

silver_stream = spark.readStream.format("delta").load(SILVER_PATH)

# -----------------------------------------------------------------------------
# Silver -> Kafka (processed.data)  
#
# Format:
# {
#   "sensor_id": "...",
#   "sensor_timestamp": "...+00:00",
#   "value": ...,
#   "unit": "...",
#   "longitude": ...,
#   "latitude": ...,
#   "district": "...",
#   "topic": "..."
# }
# -----------------------------------------------------------------------------
silver_processed_kafka = silver_stream.select(
    concat(col("topic"), lit("|"), col("district"))
    .cast(StringType())
    .alias("key"),

    to_json(struct(
        col("sensor_id"),
        col("sensor_timestamp"),
        col("value"),
        col("unit"),
        col("longitude"),
        col("latitude"),
        col("district"),
        col("topic"),
    )).alias("value"),
)

silver_kafka_query = (
    silver_processed_kafka.writeStream
    .format("kafka")
    .outputMode("append")
    .option("checkpointLocation", CP_SILVER_KAFKA)
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", PROCESSED_TOPIC)
    .start()
)


# Gold aggregation -> Delta + Kafka (business.data)

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
    .option("checkpointLocation", CP_GOLD_DELTA)
    .option("path", GOLD_PATH)
    .start()
)

# Format window timestamps like: 2026-01-15T10:30:00.000000Z
window_start_str = date_format(
    to_utc_timestamp(col("window.start"), "UTC"),
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
)
window_end_str = date_format(
    to_utc_timestamp(col("window.end"), "UTC"),
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
)

gold_business_kafka = agg.select(
    concat(col("topic"), lit("|"), col("district"))
    .cast(StringType())
    .alias("key"),

    to_json(struct(
        window_start_str.alias("window_start"),
        window_end_str.alias("window_end"),
        col("topic"),
        col("district"),
        col("avg_value"),
        col("event_count"),
    )).alias("value"),
)

gold_kafka_query = (
    gold_business_kafka.writeStream
    .format("kafka")
    .outputMode("append")
    .option("checkpointLocation", CP_GOLD_KAFKA)
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", BUSINESS_TOPIC)
    .start()
)


try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Streaming interrupted by user.", file=sys.stderr)
finally:
    for q in (silver_kafka_query, gold_kafka_query, gold_delta_query):
        try:
            if q is not None:
                q.stop()
        except Exception:
            pass
