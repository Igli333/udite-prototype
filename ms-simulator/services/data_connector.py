from typing import List, Dict, Any
from domain.models import SensorReading, Location
from datetime import datetime
import uuid

class DataConnector:
    """
    Mock Data Connector to simulate fetching data from Data Sync Service (InfluxDB/PostGIS).
    Returns SensorReading objects to match ms-data-sync schema.
    """

    def get_district_readings(self, district_id: str) -> List[SensorReading]:
        """
        Mock: Returns a list of recent sensor readings for a district.
        """
        now = datetime.now()
        readings = []

        # 1. Traffic Sensor
        readings.append(SensorReading(
            sensor_id=str(uuid.uuid4()),
            timestamp=now,
            value=85.0, # High flow
            unit="vehicles/min",
            location=Location(lat=48.8566, lng=2.3522),
            system="traffic",
            district=district_id
        ))

        # 2. Environmental Sensor (Flood check)
        readings.append(SensorReading(
            sensor_id=str(uuid.uuid4()),
            timestamp=now,
            value=80.0, # Water level %
            unit="percent",
            location=Location(lat=48.8580, lng=2.3500),
            system="environment",
            district=district_id
        ))

         # 3. Gas Sensor
        readings.append(SensorReading(
            sensor_id=str(uuid.uuid4()),
            timestamp=now,
            value=0.0, # No leak
            unit="ppm",
            location=Location(lat=48.8590, lng=2.3400),
            system="utilities",
            district=district_id
        ))
        
        # 4. Telecom Site
        readings.append(SensorReading(
            sensor_id=str(uuid.uuid4()),
            timestamp=now,
            value=1.0, # Status UP (1=UP, 0=DOWN)
            unit="status",
            location=Location(lat=48.8600, lng=2.3300),
            system="telecom",
            district=district_id
        ))


        return readings

    def get_node_details(self, node_id: str) -> Dict[str, Any]:
        """
        Mock: Returns details about a specific infrastructure node.
        """
        return {
            "node_type": "TRAFFIC_LIGHT",
            "location": {"lat": 48.8566, "lng": 2.3522},
            "status": "OFFLINE" if "fail" in node_id.lower() else "ONLINE"
        }
