FROM python:3.13-slim

WORKDIR /app

COPY app/producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app/producer/sensor_producer.py /app/sensor_producer.py

ENV PYTHONUNBUFFERED=1
CMD ["python", "sensor_producer.py"]
