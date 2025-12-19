from sqlalchemy.orm import relationship

from .sensor_data import SensorData


class TelecommunicationInfrastructureData(SensorData):
    __tablename__ = 'telecommunication_infrastructure_data'
    __table_args__ = {"schema": "public"}

    district = relationship("District", back_populates="telecommunication_infrastructure_data")
