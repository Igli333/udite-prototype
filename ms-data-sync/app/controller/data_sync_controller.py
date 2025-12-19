from flask import Blueprint, request, jsonify
from .. import service

sensors = Blueprint('sensors', __name__)
sync_service = service.DataSyncService()


@sensors.route("/add_reading", methods=['POST'])
def add_reading():
    data = request.json

    sync_service.write_sensor_reading(row=data)

    return jsonify({"success": True})
