import uuid

from .district import District
from .systems import Systems
from ..repository.postgres_base import Base

from sqlalchemy.orm import relationship
from geoalchemy2.shape import from_shape, to_shape
from sqlalchemy import Column, DateTime, String, Float
from sqlalchemy.dialects.postgresql import UUID, JSON
from geoalchemy2 import Geometry
from shapely.geometry import Point


class SensorData(Base):
    __abstract__ = True

    sensor_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, primary_key=True)
    value = Column(Float, nullable=False)
    unit = Column(String, nullable=False)
    location = Column(Geometry('POINT', srid=4326, spatial_index=True), nullable=False)
    meta_data = Column(JSON, nullable=False)
    district = relationship("District", back_populates="district")
    system = relationship("Systems", back_populates="systems")

    def __init__(self, row):
        super().__init__()
        self.sensor_id = row['sensor_id']
        self.timestamp = row['sensor_timestamp']
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
        self.meta_data = row['meta_data']
        self.system = Systems(row['system'])
        self.district = District(row['district_id'])

    def get_point(self):
        return to_shape(self.location)

    def __repr__(self):
        return f'<SensorData(sensor_id:{self.sensor_id}>, value={self.value}, time={self.timestamp}'
