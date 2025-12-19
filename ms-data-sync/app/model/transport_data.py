from sqlalchemy.orm import relationship

from .sensor_data import SensorData


class PublicTransportData(SensorData):
    __tablename__ = 'public_transport_data'
    __table_args__ = {"schema": "public"}

    district = relationship("District", back_populates="public_transport_data")
