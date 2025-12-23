from geoalchemy2 import Geometry
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship

from ..repository.postgres_base import Base


class District(Base):
    __tablename__ = 'district'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry(geometry_type='MULTIPOLYGON', srid=4326), nullable=False)

    er_data = relationship("EmergencyResourcesData", back_populates="district")
    green_data = relationship("GreenInfrastructureData", back_populates="district")
    telecom_data = relationship("TelecommunicationsInfrastructureData", back_populates="district")
    traffic_data = relationship("TrafficData", back_populates="district")
    transport_data = relationship("PublicTransportData", back_populates="district")
    utility_data = relationship("UtilitiesData", back_populates="district")
