from flask import Flask, request, jsonify

app = Flask(__name__)


def classify_event(data):
    category = data.get("category")

    # 1. Traffic
    if category == "traffic":
        avg_speed = data.get("avg_speed", 0)

        if avg_speed < 20:
            return {"event_type": "TRAFFIC_CONGESTION", "severity": "HIGH"}
        elif avg_speed < 30:
            return {"event_type": "TRAFFIC_SLOW", "severity": "LOW"}
        else:
            return {"event_type": "NO_EVENT", "severity": "NONE"}

    # 2. Public Transport
    if category == "public_transport":
        avg_delay = data.get("avg_delay_minutes", 0)
        cancelled = data.get("cancelled_trips", 0)

        if avg_delay >= 10 or cancelled > 0:
            return {"event_type": "SERVICE_DISRUPTION", "severity": "MEDIUM"}
        elif avg_delay >= 5:
            return {"event_type": "MINOR_DELAY", "severity": "LOW"}
        else:
            return {"event_type": "NO_EVENT", "severity": "NONE"}

    # 3. Environmental Conditions
    if category == "environment":
        rain = data.get("rain_mm_per_hour", 0)
        aqi = data.get("air_quality_index", 0)

        if rain >= 30:
            return {"event_type": "HEAVY_RAIN", "severity": "HIGH"}
        elif aqi >= 150:
            return {"event_type": "POOR_AIR_QUALITY", "severity": "MEDIUM"}
        else:
            return {"event_type": "NO_EVENT", "severity": "NONE"}

    # 4. Utilities (Water)
    if category == "water":
        pressure_drop = data.get("pressure_drop_percent", 0)
        flow_change = data.get("flow_rate_change_percent", 0)

        if pressure_drop >= 30:
            return {"event_type": "POSSIBLE_PIPE_BURST", "severity": "CRITICAL"}
        elif flow_change >= 20:
            return {"event_type": "WATER_ANOMALY", "severity": "MEDIUM"}
        else:
            return {"event_type": "NO_EVENT", "severity": "NONE"}

    return {"event_type": "UNKNOWN", "severity": "NONE"}


@app.route('/')
def hello_world():
    return 'Hello World! This is the event classifier of UDiTE\n'


@app.route('/classify', methods=['POST'])
def classify():
    data = request.get_json()
    result = classify_event(data)
    return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
