import os
import json
import threading

from flask import Blueprint, request, jsonify
from confluent_kafka import Consumer, Producer
from sqlalchemy.orm import sessionmaker

from .. import repository, model

KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP')

sensors = Blueprint('sensor_data', __name__)\

database = repository.Postgres()
SessionLocal = sessionmaker(bind=database.engine)

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP,
#     key_serializer=lambda k: k.encode("utf-8"),
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#     acks="all"
# )

producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    "acks": "all"
})


def read_data():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "sensor_data",
        "auto.offset.reset": "latest"
    })

    consumer.subscribe(['processed.data'])
    session = SessionLocal()

    while True:
        message = consumer.poll(timeout=1.0)

        latest_data = json.loads(message.value().decode("utf-8"))
        data = model.SensorData(latest_data)

        if message is None:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        session.add(data)
        session.commit()

        producer.produce(
            'event.classifier',
            key=data.sensor_id,
            value=json.dumps(data).encode("utf-8")
        )

        producer.poll(0)


threading.Thread(target=read_data, daemon=True).start()


@sensors.route("/syncToDataLake", methods=['POST'])
def sync_to_data_lake():
    data = request.json

    # sync_service.sync_to_data_lake(data)
    return jsonify({"success": True})
