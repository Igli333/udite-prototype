import datetime
import logging

from .. import repository, model
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.dialects.postgresql import insert


class DataSyncService:

    def __init__(self):
        self.pg_db = repository.PostGIS()
        self.db = self.pg_db.db
        self.influx = repository.InfluxDB()

    def insert_postgis_reading(self, sensor_id, value, unit, longitude, latitude, system, district, timestamp):
        point = from_shape(Point(longitude, latitude), srid=4326)

        reading = insert(model.SensorData).values(
            sensor_id=sensor_id,
            value=value,
            unit=unit,
            location=point,
            system=system,
            timestamp=timestamp,
            district=district
        )

        self.db.session.execute(reading)
        self.db.session.commit()

    def query_postgis_by_area(self, polygon_wkt, start_time=None, end_time=None, limit=100):
        query = model.SensorData.query

        if start_time:
            query = query.filter(model.SensorData.timestamp >= start_time)
        if end_time:
            query = query.filter(model.SensorData.timestamp <= end_time)
        if polygon_wkt:
            from geoalchemy2.functions import ST_Within, ST_GeomFromText
            query = query.filter(ST_Within(model.SensorData.location, ST_GeomFromText(polygon_wkt, 4326)))

        return query.order_by(model.SensorData.timestamp.desc()).limit(limit).all()

    def write_influx_points(self, bucket, points):
        try:
            self.influx.write_points(bucket, points)
        except Exception as e:
            logging.error(f"Error writing to InfluxDB: {e}")
            raise e

    def query_influx(self, query_string):
        return self.influx.query(query_string)

    def write_sensor_reading(self, sensor_id, value, unit, longitude, latitude, system, district, timestamp=None):
        timestamp = timestamp or datetime.datetime.now(datetime.timezone.utc)
        try:
            self.insert_postgis_reading(sensor_id, value, unit, longitude, latitude, system, district, timestamp)
        except Exception as e:
            logging.error(f"PostGIS write failed for sensor {sensor_id}: {e}")
            raise e

        point = {
            "measurement": "sensor_data",
            "tags": {"sensor_id": str(sensor_id), "system": system, "district": district},
            "fields": {"value": value, "unit": unit},
            "time": timestamp.isoformat()
        }

        self.write_influx_points(bucket="sensor_data", points=[point])
