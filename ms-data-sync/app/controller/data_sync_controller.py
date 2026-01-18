import json
import threading

from flask import Blueprint, request, jsonify
from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy.orm import sessionmaker

from .. import service, repository, model

sensors = Blueprint('sensor_data', __name__)
sync_service = service.DataSyncService()

database = repository.Postgres()
SessionLocal = sessionmaker(bind=database.engine)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)


def read_data():
    consumer = KafkaConsumer(
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        group_id='sensor_data',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    consumer.subscribe(topics=['processed.data'])
    session = SessionLocal()

    for message in consumer:
        latest_data = message.value  # update latest
        data = model.SensorData(latest_data)
        session.add(data)
        session.commit()

        producer.send('event.classifier', latest_data)


threading.Thread(target=read_data, daemon=True).start()


@sensors.route("/syncToDataLake", methods=['POST'])
def sync_to_data_lake():
    data = request.json

    sync_service.sync_to_data_lake(data)
    return jsonify({"success": True})
