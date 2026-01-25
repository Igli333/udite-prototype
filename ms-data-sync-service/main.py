import json
import os
import threading
import uuid
from typing import List, Optional

from confluent_kafka import Producer, Consumer, KafkaError
from fastapi import FastAPI, Query, HTTPException
from sqlalchemy.orm import sessionmaker

from model.sensor_data import SensorData
from repository.postgres import Postgres
from service import simulations_service

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)

app = FastAPI()

database = Postgres()
SessionLocal = sessionmaker(bind=database.engine)

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')

producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    "acks": "all"
})

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "sensor-data-consumer-group",
    'client.id': 'sensor-data-consumer',
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
})


def read_data():
    logger.info("Kafka consumer thread started")

    session = SessionLocal()
    try:
        consumer.subscribe(
            ["processed.data"],
            on_assign=lambda c, p: logger.info(f"Partitions assigned: {p}"),
            on_revoke=lambda c, p: logger.info(f"Partitions revoked: {p}"),
        )

        logger.info("Subscribed to topic: processed.data")
        logger.info("Waiting for partition assignment...")

        # Wait for partition assignment
        partitions_assigned = False
        while not partitions_assigned:
            message = consumer.poll(timeout=1.0)

            # Check if partitions are assigned
            assignment = consumer.assignment()
            if assignment:
                logger.info(f"Partitions now assigned: {assignment}")
                partitions_assigned = True
                break

            logger.info("Still waiting for partition assignment...")

        logger.info("Starting to consume messages...")

        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                logger.error(f"Kafka error: {message.error()}")
                continue

            try:
                message_value = message.value().decode("utf-8")
                latest_data = json.loads(message_value)

                data = SensorData(latest_data)
                session.add(data)
                session.commit()

                producer.produce(
                    'event.classifier',
                    key=data.sensor_id,
                    value=json.dumps(data).encode("utf-8")
                )
                producer.poll(0)

                logger.info(f"Processed message at offset {message.offset()}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON at offset {message.offset()}: {e}")
                logger.error(f"Raw message: {message.value()}")

            except Exception as e:
                logger.error(f"❌ Error processing message at offset {message.offset()}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error in read_data: {e}", exc_info=True)
    finally:
        logger.info("Closing consumer...")
        consumer.close()
        session.close()


threading.Thread(target=read_data, daemon=True).start()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/simulator/stream")
async def stream_sensor_data(
        system: List[str] = Query(..., description="List of systems (required)"),
        district: Optional[List[str]] = Query(None, description="List of districts"),
        last_ts: Optional[str] = Query(None, description="Last timestamp")
):
    if not system:
        raise HTTPException(status_code=400, detail="systems are required")

    return simulations_service.stream_simulation(last_ts, system, district)
