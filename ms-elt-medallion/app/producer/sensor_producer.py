import os
import json
import random
import time
import yaml
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from common.config_loader import load_config

CONFIG_PATH = os.getenv("CITY_CONFIG", "/config/city_sensors.yml")
CONFIG = load_config(CONFIG_PATH)

with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

DISTRICTS = CONFIG["districts"]  # dict: hel-01 -> {name, lat_range, lon_range}
TOPICS_CFG = CONFIG["topics"]    # dict: topic -> {unit, min_value, max_value}
CITY_CFG = CONFIG.get("city", {})

SENSORS_PER_DISTRICT_PER_TOPIC = int(CONFIG["sensors"]["per_district_per_topic"])

MESSY_CFG = CONFIG.get("messy_data", {})
MESSY_ENABLED = bool(MESSY_CFG.get("enabled", True))
CORRUPTION_PROB = float(MESSY_CFG.get("corruption_probability", 0.5))


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "5"))

TOPICS = {topic: cfg["unit"] for topic, cfg in TOPICS_CFG.items()}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    linger_ms=50,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def random_point_in_box(lat_range, lon_range):
    lat_min, lat_max = lat_range
    lon_min, lon_max = lon_range
    lat = round(random.uniform(lat_min, lat_max), 6)
    lon = round(random.uniform(lon_min, lon_max), 6)
    return lat, lon


def build_sensor_registry():
    """
    Build a fixed registry of sensors.
    Each sensor belongs to exactly one district and one topic and has a fixed lat/lon.
    """
    sensors = []
    for district_id, d in DISTRICTS.items():
        district_name = d["name"]
        lat_range = tuple(d["lat_range"])
        lon_range = tuple(d["lon_range"])

        for topic, tcfg in TOPICS_CFG.items():
            unit = tcfg["unit"]
            base = topic.split(".")[0]

            for i in range(1, SENSORS_PER_DISTRICT_PER_TOPIC + 1):
                lat, lon = random_point_in_box(lat_range, lon_range)
                sensor_id = f"{base}-{district_id}-{i:02d}"
                sensors.append(
                    {
                        "sensor_id": sensor_id,
                        "topic": topic,
                        "unit": unit,
                        "district_id": district_id,
                        "district_name": district_name,
                        "latitude": lat,
                        "longitude": lon,
                    }
                )
    return sensors


SENSORS = build_sensor_registry()


def make_clean_sensor_record(sensor: dict) -> dict:
    """
    Make a clean record from a fixed sensor.
    Value is generated in a broad range; Silver enforces final constraints.
    """
    return {
        "sensor_id": sensor["sensor_id"],
        "sensor_timestamp": utc_now_iso(),
        "value": round(random.uniform(10, 100), 4),
        "unit": sensor["unit"],
        "longitude": sensor["longitude"],
        "latitude": sensor["latitude"],
        "district_id": sensor["district_id"],
        "district_name": sensor["district_name"],
        "topic": sensor["topic"],
    }


def maybe_corrupt_record(record: dict):
    """
    Randomly corrupt a record.
    Returns a list of records (to allow duplicates).
    CORRUPTION_PROB controls how often *any* corruption happens.
    """
    if not MESSY_ENABLED:
        return [record]

    if random.random() > CORRUPTION_PROB:
        return [record]  

    r = random.random()
    records_to_send = [record]

    # 0–0.10: null value
    if r < 0.10:
        record["value"] = None

    # 0.10–0.20: invalid timestamp format
    elif r < 0.20:
        record["sensor_timestamp"] = "not-a-timestamp"

    # 0.20–0.30: future timestamp (Silver should drop if beyond allowed skew)
    elif r < 0.30:
        minutes_ahead = random.randint(10, 240)
        fut = datetime.now(timezone.utc) + timedelta(minutes=minutes_ahead)
        record["sensor_timestamp"] = fut.isoformat()

    # 0.30–0.40: coordinates completely wrong (outside Helsinki bounds)
    elif r < 0.40:
        record["latitude"] = 0.0
        record["longitude"] = 0.0

    # 0.40–0.50: missing unit
    elif r < 0.50:
        record.pop("unit", None)

    # 0.50–0.60: unknown district id
    elif r < 0.60:
        record["district_id"] = "hel-99"

    # 0.60–0.70: value as string instead of number (Spark schema -> null)
    elif r < 0.70:
        if record.get("value") is not None:
            record["value"] = str(record["value"])

    # 0.70–0.80: extreme outlier
    elif r < 0.80:
        record["value"] = random.choice([-9999.0, 9999.0, 1e6])

    # 0.80–0.90: invalid topic
    elif r < 0.90:
        record["topic"] = "unknown.sensors"

    # 0.90–1.00: introduce a duplicate with slightly changed value
    else:
        dup = record.copy()
        if isinstance(dup.get("value"), (int, float)):
            dup["value"] = dup["value"] + random.uniform(-1, 1)
        records_to_send.append(dup)

    return records_to_send


def main():
    print(f"Producer connecting to {KAFKA_BOOTSTRAP}")
    print(f"Config: {CONFIG_PATH}")
    print(f"Districts: {len(DISTRICTS)} | Topics: {len(TOPICS_CFG)}")
    print(f"Sensors per district per topic: {SENSORS_PER_DISTRICT_PER_TOPIC}")
    print(f"Total sensors in registry: {len(SENSORS)}")
    print(f"Messy enabled: {MESSY_ENABLED} | corruption_probability: {CORRUPTION_PROB}")

    while True:
        # send from a random subset each heartbeat
        batch_size = random.randint(10, min(50, len(SENSORS)))
        sensors_this_batch = random.sample(SENSORS, k=batch_size)

        for sensor in sensors_this_batch:
            clean = make_clean_sensor_record(sensor)
            for rec in maybe_corrupt_record(clean):
                key = rec.get("sensor_id", "null")
                
                kafka_topic = rec.get("topic") or sensor["topic"]
                producer.send(kafka_topic, key=key, value=rec)

        producer.flush()
        print(f"[{utc_now_iso()}] sent batch (~{batch_size} sensors)")
        time.sleep(HEARTBEAT_SECONDS)


if __name__ == "__main__":
    main()
