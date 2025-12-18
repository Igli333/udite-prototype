from flask import Blueprint, request, jsonify
from .. import service

sensors = Blueprint('sensors', __name__)
sync_service = service.DataSyncService()

@sensors.route("/add_reading", methods=['POST'])
def add_reading():
    data = request.json

    sync_service.write_sensor_reading(
        sensor_id=data["sensor_id"],
        value=data["value"],
        unit=data["unit"],
        latitude=data["latitude"],
        longitude=data["longitude"],
        system=data["system"],
        district=data["district"],
        timestamp=data["timestamp"]
    )

    return jsonify({"success": True})