import threading
from influxdb_client import InfluxDBClient, WriteOptions, QueryApi, Point

class InfluxDB:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, url=None, token=None, org=None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize(url, token, org)
        return cls._instance

    def _initialize(self, url, token, org):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=WriteOptions(flush_interval=10_000))
        self.query_api = self.client.query_api()

    def write_points(self, bucket, points):
        self.write_api.write(bucket=bucket, record=points)

    def query(self, query):
        return self.query_api.query(query=query)

with InfluxDBClient(url='http://influxdb:8086', token='TOKEN', org='UDITE') as client:
    write_api = client.write_api(write_options=WriteOptions(batch_size=5000, flush_interval=10_000))
    write_api.write(bucket='sensor_data', record=[])