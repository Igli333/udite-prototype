import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

TOPICS = {
    "traffic.sensors": "km/h",
    "transport.sensors": "passengers",
    "utilities.sensors": "kWh",
    "environment.sensors": "ppm",
    "telecom.sensors": "dBm",
    "greeninfra.sensors": "liters",
}

HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "5"))  

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    linger_ms=50,
)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def make_sensor_record(topic: str, unit: str) -> dict:
    base = topic.split(".")[0]
    return {
        "sensor_id": f"{base}-{random.randint(1, 50)}",
        "sensor_timestamp": utc_now_iso(),  # ISO string
        "value": round(random.uniform(10, 100), 4),
        "unit": unit,
        "longitude": round(23.7 + random.random() * 0.05, 6),
        "latitude": round(37.98 + random.random() * 0.05, 6),
        "district": str(random.randint(1, 5)),
        "topic": topic,  
    }

def main():
    print(f"Producer connecting to {KAFKA_BOOTSTRAP}")
    while True:
        for topic, unit in TOPICS.items():
            # publish 1–3 events per topic per heartbeat
            for _ in range(random.randint(1, 3)):
                payload = make_sensor_record(topic, unit)
                key = payload["sensor_id"]
                producer.send(topic, key=key, value=payload)
        producer.flush()
        print(f"[{utc_now_iso()}] sent batch")
        time.sleep(HEARTBEAT_SECONDS)

if __name__ == "__main__":
    main()
