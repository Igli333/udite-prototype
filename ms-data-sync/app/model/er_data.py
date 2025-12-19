from sqlalchemy import Column, String
from sqlalchemy.orm import relationship

from .sensor_data import SensorData


class EmergencyResourcesData(SensorData):
    __tablename__ = 'emergency_resources_data'
    __table_args__ = {"schema": "public"}

    type = Column(String, nullable=False)

    district = relationship("District", back_populates="emergency_resources_data")
