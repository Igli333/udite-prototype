import os
import numpy as np
import json
import time
import requests
import redis
import threading

from flask import Flask, jsonify
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer

from .app import map_sensor_to_category, compute_trends_category, classify_event_streaming

app = Flask(__name__)

r = redis.Redis(host="redis", port=6379, db=0)
WINDOW_SECONDS = 10 * 60  # 10-minute sliding window


def push_event(sensor_id, sensor_timestamp, value, unit=None):
    key = f"sensor:{sensor_id}:events"

    # Convert ISO timestamp to epoch seconds
    ts_epoch = int(datetime.fromisoformat(sensor_timestamp).timestamp())

    payload = {
        "timestamp": sensor_timestamp,
        "value": value,
        "unit": unit
    }

    # Add to sorted set with epoch as score
    r.zadd(key, {json.dumps(payload): ts_epoch})

    # Remove old entries beyond 10 min
    cutoff = ts_epoch - WINDOW_SECONDS
    r.zremrangebyscore(key, 0, cutoff)


def get_last_window(sensor_id):
    key = f"sensor:{sensor_id}:events"
    cutoff = int(time.time()) - WINDOW_SECONDS
    items = r.zrangebyscore(key, cutoff, "+inf")
    return [json.loads(x) for x in items]


def compute_trends(window):
    """
    Compute basic trend metrics: avg, min, max, slope
    """
    if not window:
        return {}

    values = np.array([d["value"] for d in window])
    times = np.array([datetime.fromisoformat(d["timestamp"]).timestamp() for d in window])

    avg_val = float(np.mean(values))
    min_val = float(np.min(values))
    max_val = float(np.max(values))

    if len(values) >= 2:
        slope = float(np.polyfit(times - times[0], values, 1)[0])
    else:
        slope = 0.0

    return {
        "avg": avg_val,
        "min": min_val,
        "max": max_val,
        "slope": slope
    }


executor = ThreadPoolExecutor(max_workers=5)
SIMULATOR_URL = os.getenv("SIMULATOR_URL")


def push_to_simulator(event):
    # For now, just print
    print(f"Pushing event to simulator: {event}")
    requests.post(SIMULATOR_URL, json=event)  # Uncomment for real push


def process_message(msg):
    try:
        data = json.loads(msg.value().decode("utf-8"))
        sensor_id = data["sensor_id"]
        sensor_timestamp = data["sensor_timestamp"]
        value = data["value"]
        unit = data.get("unit")

        # Map sensor to category
        category = map_sensor_to_category(sensor_id, unit)

        # Push to Redis sliding window
        push_event(sensor_id, sensor_timestamp, value, unit)

        # Get last 10-min window
        window = get_last_window(sensor_id)

        # Compute trend metrics for category
        trend_metrics = compute_trends_category(window, category)

        # Classify event
        event = classify_event_streaming(sensor_id, category, trend_metrics)

        # Push to simulator if severity is high/medium
        if event["severity"] in ["HIGH", "MEDIUM"]:
            executor.submit(push_to_simulator, event)

    except Exception as e:
        print(f"Error processing message: {e}")


def kafka_consumer_thread():
    consumer = KafkaConsumer(
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        group_id='sensor_data',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    consumer.subscribe(topics=['event.classifier'])

    for message in consumer:
        latest_data = message.value


# Start Kafka consumer in a background thread when Flask starts
threading.Thread(target=kafka_consumer_thread, daemon=True).start()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/window/<sensor_id>", methods=["GET"])
def window(sensor_id):
    """
    Optional endpoint to inspect the last 10-minute window for a sensor
    """
    window_data = get_last_window(sensor_id)
    return jsonify({"sensor_id": sensor_id, "window": window_data})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
