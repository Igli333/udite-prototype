from sqlalchemy.orm import relationship

from .sensor_data import SensorData


class TrafficData(SensorData):
    __tablename__ = 'traffic_data'
    __table_args__ = {"schema": "public"}

    district = relationship("District", back_populates="traffic_data")
