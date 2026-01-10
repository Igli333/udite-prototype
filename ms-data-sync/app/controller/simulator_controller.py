from flask import Blueprint, request, jsonify

from .. import service

simulator = Blueprint('simulator', __name__)
sync_service = service.DataSyncService()


@simulator.route('/simulator/stream', methods=['GET'])
def stream_sensor_data():
    systems = request.args.getlist("system")
    districts = request.args.getlist("district")
    last_ts = request.args.get("last_ts")

    if not systems:
        return {"error": "systems are required"}, 400

    return jsonify(service.stream_simulation(last_ts, systems, districts))
