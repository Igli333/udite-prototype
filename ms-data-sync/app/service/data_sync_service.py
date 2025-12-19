import logging

from .. import repository, model

class DataSyncService:

    def __init__(self):
        self.pg_db = repository.PostGIS()
        self.influx = repository.InfluxDB().client

    def insert_postgis_reading(self, row):
        sensor_data = model.SensorData(row)

        self.pg_db.session.add(sensor_data)
        self.pg_db.session.commit()

