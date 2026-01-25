import os
import json
import threading
import traceback

from confluent_kafka import Consumer, Producer
from sqlalchemy.orm import sessionmaker

from flask import Flask
from app.controller import simulator
from app.repository import Postgres
from app.model import SensorData

app = Flask(__name__)

app.register_blueprint(simulator)

KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP')

database = Postgres()
SessionLocal = sessionmaker(bind=database.engine)

producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    "acks": "all"
})

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "sensor_data_debug",
    "auto.offset.reset": "earliest"
})

consumer_thread = None


def start_consumer():
    global consumer_thread

    if consumer_thread is None:
        print("starting Kafka consumer thread")
        consumer_thread = threading.Thread(
            target=read_data,
            name="kafka-consumer",
            daemon=False
        )
        consumer_thread.start()


def on_assign(consumer, partitions):
    print("partitions assigned:", partitions)


def read_data():
    try:
        print("it is working")
        session = SessionLocal()

        consumer.subscribe(['processed.data'], on_assign=on_assign)
        consumer.subscribe(['processed.data'])

        while True:
            print("polling...")
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            print("received: ", message.value())

            latest_data = json.loads(message.value().decode("utf-8"))
            data = SensorData(latest_data)

            session.add(data)
            session.commit()

            producer.produce(
                'event.classifier',
                key=data.sensor_id,
                value=json.dumps(data).encode("utf-8")
            )

            producer.poll(0)
    except Exception:
        print("consumer thread crashed")
        traceback.print_exc()
        consumer.close()


@app.route('/')
def hello_world():
    return 'Hello World! This is the Data Synchronizer of UDiTE\n'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=False, use_reloader=False)
    start_consumer()
