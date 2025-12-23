from sqlalchemy.orm import relationship

from .sensor_data import SensorData


class GreenInfrastructureData(SensorData):
    __tablename__ = 'green_infrastructure_data'
    __table_args__ = {"schema": "public"}

    district = relationship("District", back_populates="green_infrastructure_data")
