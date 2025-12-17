from flask import Flask, request, jsonify
from pydantic import ValidationError
from domain.models import SimulationRequest
from services.simulation_engine import SimulationEngine

app = Flask(__name__)
simulation_engine = SimulationEngine()

@app.route('/')
def hello_world():
    return 'Hello World! This is the simulator of UDiTE\n'

@app.route('/simulate', methods=['POST'])
def trigger_simulation():
    try:
        data = request.get_json()
        sim_request = SimulationRequest(**data)
        
        result = simulation_engine.run_simulation(sim_request)
        
        return jsonify(result.model_dump()), 200
    except ValidationError as e:
        return jsonify({"error": e.errors()}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)


# Hit: 

# curl -X POST http://localhost:8000/simulate \
# -H "Content-Type: application/json" \
# -d '{"trigger_type": "NODE_FAILURE", "target_id": "Traffic_Light_1"}'

# curl -X POST http://localhost:8000/simulate \
# -H "Content-Type: application/json" \
# -d '{"trigger_type": "ENVIRONMENTAL_ALERT", "target_id": "City_Center"}'