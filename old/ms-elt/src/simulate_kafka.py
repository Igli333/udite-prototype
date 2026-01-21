# Databricks notebook source
import time, random
from datetime import datetime
from pyspark.sql.functions import current_timestamp, to_json, struct, lit

topics = {
    "traffic.sensors": "km/h",
    "transport.sensors": "passengers",
    "utilities.sensors": "kWh",
    "environment.sensors": "ppm",
    "telecom.sensors": "dBm",
    "greeninfra.sensors": "liters"
}

schema = [
    "sensor_id", "sensor_timestamp", "value",
    "unit", "longitude", "latitude", "district", "topic"
]

while True:
    rows = []

    for topic, unit in topics.items():
        for _ in range(random.randint(1, 3)):
            rows.append((
                f"{topic.split('.')[0]}-{random.randint(1,50)}",
                datetime.utcnow(),
                random.uniform(10, 100),
                unit,
                23.7 + random.random() * 0.05,
                37.98 + random.random() * 0.05,
                str(random.randint(1, 5)),
                topic
            ))

    df = spark.createDataFrame(rows, schema)
    
    bronze_df = df.select(
        to_json(struct("*")).alias("value"),
        "topic"
    ).withColumn("ingest_time", current_timestamp())

    bronze_df.write.format("delta") \
        .mode("append") \
        .saveAsTable("workspace4sadt.bronze.sensors_raw")

    time.sleep(15)  # 15-second heartbeat




