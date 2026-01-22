from geoalchemy2 import Geometry
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from ..repository.postgres_base import Base


class District(Base):
    __tablename__ = 'district'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry(geometry_type='POLYGON', srid=4326), nullable=False)

    sensor_data = relationship("SensorData", back_populates="sensor_data")
