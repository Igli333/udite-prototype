from sqlalchemy import Column, String
from sqlalchemy.orm import relationship

from .sensor_data import SensorData


class UtilitiesData(SensorData):
    __tablename__ = 'utilities_data'
    __table_args__ = {"schema": "public"}

    utility_type = Column(String)

    district = relationship("District", back_populates="utilities_data")

    def __init__(self, row):
        super().__init__(row)
        self.utility_type = row["utility_type"]
