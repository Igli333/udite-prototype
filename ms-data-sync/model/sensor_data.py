import uuid

from .. import repository
from sqlalchemy.dialects.postgresql import UUID
from geoalchemy2 import Geometry

db = repository.PostGIS().db


class SensorData(db.Model):
    __tablename__ = 'sensor_data'

    sensor_id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = db.Column(db.DateTime, primary_key=True)
    value = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String, nullable=False)
    location = db.Column(Geometry('POINT', srid=4326), nullable=False)
    system = db.Column(db.String, nullable=False)
    district = db.Column(db.String, nullable=False)

    def __repr__(self):
        return f'<SensorData(sensor_id:{self.sensor_id}>, value={self.value}, time={self.timestamp}'
