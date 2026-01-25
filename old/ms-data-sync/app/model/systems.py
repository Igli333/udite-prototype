from geoalchemy2 import Geometry
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from ..repository.postgres_base import Base


class Systems(Base):
    __tablename__ = 'Systems'
    id = Column(String, primary_key=True)
    name = Column(String)
    description = CColumn(String)

    sensor_data = relationship("SensorData", back_populates="sensor_data")
