import os
from pyspark.sql.functions import col, current_timestamp
from app.common.spark_session import get_spark

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = "traffic.sensors,transport.sensors,utilities.sensors,environment.sensors,telecom.sensors,greeninfra.sensors"

BRONZE_PATH = "/opt/data/bronze/sensors_raw"
CHECKPOINT = "/opt/checkpoints/bronze/sensors_raw"

os.makedirs(BRONZE_PATH, exist_ok=True)
os.makedirs(CHECKPOINT, exist_ok=True)

spark = get_spark("bronze-kafka-to-delta")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPICS)               
    .option("startingOffsets", "latest")
    .load()
)

bronze = df.select(
    col("topic").cast("string").alias("topic"),
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingest_time"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
)

query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("path", BRONZE_PATH)   
    .start()
)

query.awaitTermination()
