from typing import Dict, Any

class DataConnector:
    """
    Mock Data Connector to simulate fetching data from Data Sync Service (InfluxDB/PostGIS).
    In a real implementation, this would make HTTP requests to ms-data-sync.
    """

    def get_district_status(self, district_id: str) -> Dict[str, Any]:
        """
        Mock: Returns current status of a district.
        """
        # Mocking generic district data
        return {
            "traffic_density": 0.75, # 75%
            "weather_condition": "rainy",
            "active_alerts": 2
        }

    def get_node_details(self, node_id: str) -> Dict[str, Any]:
        """
        Mock: Returns details about a specific infrastructure node (e.g., Traffic Light).
        """
        return {
            "node_type": "TRAFFIC_LIGHT",
            "location": {"lat": 48.8566, "lng": 2.3522},
            "status": "OFFLINE" if "fail" in node_id.lower() else "ONLINE"
        }
