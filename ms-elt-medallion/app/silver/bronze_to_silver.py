import os
import yaml

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    row_number,
    expr,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from app.common.spark_session import get_spark

# ----------------------------
# Config loading
# ----------------------------
CONFIG_PATH = os.getenv("CITY_CONFIG", "/opt/config/city_sensors.yml")
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

TOPICS_CFG = CONFIG["topics"]
VALID_TOPICS = list(TOPICS_CFG.keys())
VALID_DISTRICTS = list(CONFIG["districts"].keys())

CITY_LAT_MIN, CITY_LAT_MAX = CONFIG["city"]["lat_range"]
CITY_LON_MIN, CITY_LON_MAX = CONFIG["city"]["lon_range"]

MAX_FUTURE_MINUTES = int(CONFIG["time_rules"]["max_future_minutes"])
MAX_PAST_DAYS = int(CONFIG["time_rules"]["max_past_days"])

# ----------------------------
# Paths
# ----------------------------
BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/data/bronze/sensors_raw")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/data/silver/sensors_cleaned")
CHECKPOINT = os.getenv("SILVER_CHECKPOINT", "/opt/checkpoints/silver/sensors_cleaned")

os.makedirs(SILVER_PATH, exist_ok=True)
os.makedirs(CHECKPOINT, exist_ok=True)

payload_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("sensor_timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("district_id", StringType(), True),
        StructField("district_name", StringType(), True),
        StructField("topic", StringType(), True),
    ]
)

spark = get_spark("silver-bronze-to-silver")


def value_in_range_expr():
    """
    Build a CASE WHEN expression from config:
      WHEN topic='traffic.sensors' THEN value BETWEEN min AND max
      ...
    """
    clauses = []
    for topic, cfg in TOPICS_CFG.items():
        min_v = cfg["min_value"]
        max_v = cfg["max_value"]
        clauses.append(
            f"WHEN topic = '{topic}' THEN value >= {min_v} AND value <= {max_v}"
        )
    return expr("CASE " + " ".join(clauses) + " ELSE false END")


def unit_matches_topic_expr():
    """
    Optional: enforce unit matches topic, e.g. traffic.sensors -> km/h.
    If unit is missing or mismatched, drop.
    """
    clauses = []
    for topic, cfg in TOPICS_CFG.items():
        unit = cfg["unit"]
        # accept exact match only
        clauses.append(f"WHEN topic = '{topic}' THEN unit = '{unit}'")
    return expr("CASE " + " ".join(clauses) + " ELSE false END")


def upsert_to_silver(microbatch: DataFrame, batch_id: int) -> None:
    """
    MERGE microbatch into Silver Delta.
    Primary key: (topic, sensor_id, sensor_timestamp)
    Keep latest by ingest_time.
    """
    if microbatch.rdd.isEmpty():
        return

    w = (
        Window.partitionBy("topic", "sensor_id", "sensor_timestamp")
        .orderBy(col("ingest_time").desc())
    )
    deduped = (
        microbatch.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(
                deduped.alias("s"),
                "t.topic = s.topic AND t.sensor_id = s.sensor_id AND t.sensor_timestamp = s.sensor_timestamp",
            )
            .whenMatchedUpdateAll(condition="s.ingest_time >= t.ingest_time")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        deduped.write.format("delta").mode("overwrite").save(SILVER_PATH)


bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

parsed = bronze_stream.withColumn("payload", from_json(col("value"), payload_schema))

silver = (
    parsed.select(
        # Kafka metadata from bronze
        col("topic").alias("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("ingest_time"),

        # Parsed payload
        col("payload.sensor_id").alias("sensor_id"),
        col("payload.sensor_timestamp").alias("sensor_timestamp"),
        col("payload.value").alias("value"),
        col("payload.unit").alias("unit"),
        col("payload.longitude").alias("longitude"),
        col("payload.latitude").alias("latitude"),
        col("payload.district_id").alias("district_id"),
        col("payload.district_name").alias("district_name"),
        col("payload.topic").alias("payload_topic"),
    )
    # prefer payload.topic; if missing, keep kafka_topic as fallback
    .withColumn("topic", expr("coalesce(payload_topic, kafka_topic)"))
    .drop("payload_topic")
    .withColumn("event_time", to_timestamp(col("sensor_timestamp")))
    .withColumn("silver_ingest_time", current_timestamp())
)

# Build validity flags (strict cleaning)
silver_validated = (
    silver
    # required fields
    .withColumn("ok_sensor_id", col("sensor_id").isNotNull())
    .withColumn("ok_topic", col("topic").isNotNull())
    .withColumn("ok_event_time", col("event_time").isNotNull())
    .withColumn("ok_value", col("value").isNotNull())
    .withColumn("ok_unit", col("unit").isNotNull())
    .withColumn("ok_district", col("district_id").isNotNull())
    .withColumn("ok_latlon", col("latitude").isNotNull() & col("longitude").isNotNull())

    # membership checks
    .withColumn("ok_topic_known", col("topic").isin(VALID_TOPICS))
    .withColumn("ok_district_known", col("district_id").isin(VALID_DISTRICTS))

    # geo bounds (Helsinki)
    .withColumn(
        "ok_latlon_bounds",
        col("latitude").between(CITY_LAT_MIN, CITY_LAT_MAX)
        & col("longitude").between(CITY_LON_MIN, CITY_LON_MAX)
    )

    # time sanity: not too future, not too old
    .withColumn(
        "ok_not_future",
        col("event_time") <= current_timestamp() + expr(f"INTERVAL {MAX_FUTURE_MINUTES} MINUTES")
    )
    .withColumn(
        "ok_not_too_old",
        col("event_time") >= current_timestamp() - expr(f"INTERVAL {MAX_PAST_DAYS} DAYS")
    )

    # value ranges per topic
    .withColumn("ok_value_range", value_in_range_expr())

    # unit matches topic
    .withColumn("ok_unit_match", unit_matches_topic_expr())

    # final pass/fail
    .withColumn(
        "is_valid_record",
        col("ok_sensor_id")
        & col("ok_topic")
        & col("ok_event_time")
        & col("ok_value")
        & col("ok_unit")
        & col("ok_district")
        & col("ok_latlon")
        & col("ok_topic_known")
        & col("ok_district_known")
        & col("ok_latlon_bounds")
        & col("ok_not_future")
        & col("ok_not_too_old")
        & col("ok_value_range")
        & col("ok_unit_match")
    )
)

# Final Silver projection: district must be ID (hel-xx)
silver_clean = (
    silver_validated
    .filter(col("is_valid_record"))
    .select(
        col("sensor_id"),
        col("sensor_timestamp"),
        col("value"),
        col("unit"),
        col("longitude"),
        col("latitude"),
        col("district_id").alias("district"),  # district id only
        col("topic"),
        col("event_time"),
        col("silver_ingest_time"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("ingest_time"),
    )
)

query = (
    silver_clean.writeStream
    .foreachBatch(upsert_to_silver)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

query.awaitTermination()
