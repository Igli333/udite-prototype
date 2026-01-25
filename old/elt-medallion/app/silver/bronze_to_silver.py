import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, row_number, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.window import Window

from delta.tables import DeltaTable
from app.common.spark_session import get_spark

# Paths
BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/data/bronze/sensors_raw")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/data/silver/sensors_cleaned")
CHECKPOINT = os.getenv("SILVER_CHECKPOINT", "/opt/checkpoints/silver/sensors_cleaned")

os.makedirs(SILVER_PATH, exist_ok=True)
os.makedirs(CHECKPOINT, exist_ok=True)

# JSON schema of your payload
payload_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("sensor_timestamp", StringType(), True),  
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("district", StringType(), True),
    StructField("topic", StringType(), True),
])

spark = get_spark("silver-bronze-to-silver")

def upsert_to_silver(microbatch: DataFrame, batch_id: int) -> None:
    """
    MERGE microbatch into the Silver Delta table.
    Primary key: (topic, sensor_id, sensor_timestamp)
    Keep the latest by ingest_time.
    """
    if microbatch.rdd.isEmpty():
        return

    # Deduplicate within the microbatch by latest ingest_time
    w = Window.partitionBy("topic", "sensor_id", "sensor_timestamp").orderBy(col("ingest_time").desc())
    deduped = (
        microbatch
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        target = DeltaTable.forPath(spark, SILVER_PATH)
        (
            target.alias("t")
            .merge(
                deduped.alias("s"),
                "t.topic = s.topic AND t.sensor_id = s.sensor_id AND t.sensor_timestamp = s.sensor_timestamp"
            )
            .whenMatchedUpdateAll(condition="s.ingest_time >= t.ingest_time")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (deduped.write.format("delta").mode("overwrite").save(SILVER_PATH))

bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

# Parse JSON payload in bronze.value
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
        col("payload.district").alias("district"),

        # prefer payload.topic if present, else kafka topic
        col("payload.topic").alias("payload_topic"),
    )
    .withColumn("topic", col("payload_topic"))
    .drop("payload_topic")
    .withColumn("event_time", to_timestamp(col("sensor_timestamp")))
    .withColumn("silver_ingest_time", current_timestamp())
)

silver_clean = (
    silver
    .filter(col("sensor_id").isNotNull())
    .filter(col("event_time").isNotNull())
    .filter(col("value").isNotNull())
    .filter(col("topic").isNotNull())
)

query = (
    silver_clean.writeStream
    .foreachBatch(upsert_to_silver)
    .outputMode("update")  
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

query.awaitTermination()
