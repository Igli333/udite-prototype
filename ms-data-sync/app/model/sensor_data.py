import uuid

from ..repository.postgis_base import Base
from geoalchemy2.shape import from_shape, to_shape
from sqlalchemy import Column, DateTime, String, Float
from sqlalchemy.dialects.postgresql import UUID
from geoalchemy2 import Geometry
from shapely.geometry import Point

class SensorData(Base):
    __tablename__ = 'sensor_data'
    __table_args__ = {"schema": "public"}

    sensor_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, primary_key=True)
    value = Column(Float, nullable=False)
    unit = Column(String, nullable=False)
    location = Column(Geometry('POINT', srid=4326, spatial_index=True), nullable=False)
    system = Column(String, nullable=False)
    district = Column(String, nullable=False)

    def __init__(self, row):
        self.sensor_id = row['sensor_id']
        self.timestamp = row['timestamp']
        self.value = float(row['value'])
        self.unit = row['unit']
        self.location = from_shape(
            Point(
                float(row["longitude"]),
                float(row["latitude"])
            ),
            srid=4326,
            spatial_index=True
        )
        self.system = row['system']
        self.district = row['district']



    def get_point(self):
        return to_shape(self.location)

    def __repr__(self):
        return f'<SensorData(sensor_id:{self.sensor_id}>, value={self.value}, time={self.timestamp}'
