from flask import Blueprint, request, jsonify
from .. import service

sensors = Blueprint('sensors', __name__)
sync_service = service.DataSyncService()


@sensors.route("/addReading", methods=['POST'])
def add_reading():
    data = request.json

    sync_service.insert_postgis_reading(data)

    return jsonify({"success": True})


@sensors.route("/syncToDataLake", methods=['POST'])
def sync_to_data_lake():
    data = request.json

    sync_service.sync_to_data_lake(data)
    return jsonify({"success": True})
