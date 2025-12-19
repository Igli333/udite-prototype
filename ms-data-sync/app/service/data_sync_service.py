import logging

from .. import repository, model
from influxdb_client import Point


class DataSyncService:

    def __init__(self):
        self.pg_db = repository.PostGIS()
        self.influx = repository.InfluxDB().client

    def insert_postgis_reading(self, row):
        sensor_data = model.SensorData(row)

        self.pg_db.session.add(sensor_data)
        self.pg_db.session.commit()

    def write_influx_points(self, bucket, row):
        point = (
            Point("sensor_reading")
            .tag("sensor_id", row["sensor_id"])
            .tag("system", row["system"])
            .tag("district", row["district"])
            .field("value", row["value"])
            .field("unit", row["unit"])
            .time("timestamp", row["timestamp"])
        )

        try:
            self.influx.write_points(bucket, point)
        except Exception as e:
            logging.error(f"Error writing to InfluxDB: {e}")
            raise e

    def write_sensor_reading(self, row):
        try:
            self.insert_postgis_reading(row)
        except Exception as e:
            logging.error(f"PostGIS write failed for sensor {row['sensor_id']}: {e}")
            raise e

        self.write_influx_points(bucket="sensor_data", row=row)
