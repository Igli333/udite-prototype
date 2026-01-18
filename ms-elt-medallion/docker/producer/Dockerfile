FROM python:3.13-slim

WORKDIR /app

COPY app/producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app/producer/ /app/
COPY app/common/ /app/common/
COPY config/city_sensors.yml /config/city_sensors.yml

ENV PYTHONUNBUFFERED=1
CMD ["python", "sensor_producer.py"]
