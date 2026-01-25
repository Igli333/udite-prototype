# ELT Medallion Pipeline

A containerized Extract-Load-Transform (ELT) pipeline using the Medallion architecture pattern with Kafka, Delta Lake, and Apache Spark. Processes multi-domain sensor data through Bronze (raw), Silver (cleaned), and Gold (aggregated) layers.

## Quick Start

### 1. Start Infrastructure

Tear down any existing containers and start fresh (under the elt-medallion directory):

```bash
docker compose up -d
```

### 2. Create Kafka Topics

Create the required Kafka topics for all sensor domains and the output topic:

```bash
docker exec -it kafka bash -lc '
for t in traffic.sensors transport.sensors utilities.sensors environment.sensors telecom.sensors greeninfra.sensors processed.data business.data; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$t" --partitions 1 --replication-factor 1
done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | sort
'
```

## Pipeline Stages

Run each stage in separate terminals:

### Bronze Layer - Kafka to Delta Lake

Ingest raw sensor data from Kafka topics into Delta Lake format:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/bronze/kafka_to_delta.py
```

### Silver Layer - Transform & Deduplicate

Parse JSON payloads, deduplicate records, and store in Delta Lake:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/app/silver/bronze_to_silver.py
```

### Gold Layer - Aggregate & Publish

Compute 1-minute windowed aggregations and publish to Kafka and Delta Lake:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/gold/silver_to_kafka.py
```

## Monitoring & Verification

### Check Delta Lake Tables

Verify that Delta Lake tables are being created and updated:

```bash
docker exec -it spark bash -lc "
ls -la /opt/data/bronze/sensors_raw/_delta_log 2>/dev/null | head || echo 'No bronze delta yet';
ls -la /opt/data/silver/sensors_cleaned/_delta_log 2>/dev/null | head || echo 'No silver delta yet';
ls -la /opt/data/gold/sensors_gold/_delta_log 2>/dev/null | head || echo 'No gold delta yet';
"
```

### View Processed Data

Consume messages from the Gold output Kafka topic:

```bash
docker exec -it kafka bash -lc \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic processed.data --from-beginning --timeout-ms 10000 | head -n 20"
```

## Web Interfaces
- **Kafka UI**: http://localhost:8080


## Data Architecture

**Bronze → Silver → Gold Pipeline:**

1. **Bronze**: Raw events from Kafka with metadata (partition, offset, timestamp)
2. **Silver**: Parsed JSON, deduplicated by (topic, sensor_id, timestamp), enriched with event_time
3. **Gold**: 1-minute windowed aggregations (avg_value, event_count) by topic and district

## Kafka Topics

### Source Topics (Sensor Data)

| Topic | Unit | Description |
|-------|------|-------------|
| `traffic.sensors` | km/h | Traffic speed sensor readings |
| `transport.sensors` | passengers | Public transport passenger counts |
| `utilities.sensors` | kWh | Energy consumption readings |
| `environment.sensors` | ppm | Environmental pollutant measurements |
| `telecom.sensors` | dBm | Telecommunications signal strength |
| `greeninfra.sensors` | liters | Green infrastructure water measurements |

### Output Topic

| Topic | Description |
|-------|-------------|
| `processed.data` | 1-minute windowed aggregations from Gold layer |

## Sensor Data JSON Schema

All sensor events follow this JSON structure:

```json
{
  "sensor_id": "traffic-42",
  "sensor_timestamp": "2026-01-15T10:30:45.123456+00:00",
  "value": 65.4321,
  "unit": "km/h",
  "longitude": 23.726543,
  "latitude": 37.984321,
  "district": "3",
  "topic": "traffic.sensors"
}
```

**Field Descriptions:**

- `sensor_id`: Unique sensor identifier (format: `{domain}-{1-50}`)
- `sensor_timestamp`: ISO 8601 timestamp of measurement in UTC
- `value`: Numeric measurement value
- `unit`: Unit of measurement (domain-specific)
- `longitude`: Geographic longitude coordinate
- `latitude`: Geographic latitude coordinate
- `district`: District identifier (1-5)
- `topic`: Source Kafka topic name

## Gold Layer Output Schema

Cleaned and transformed data retrieved from silver publichsed to `processed.data` topic:

```json
{
  "sensor_id": "traffic-42",
  "sensor_timestamp": "2026-01-15T10:30:45.123456+00:00",
  "value": 65.4321,
  "unit": "km/h",
  "longitude": 23.726543,
  "latitude": 37.984321,
  "district": "3",
  "topic": "traffic.sensors"
}
```

Aggregated messages published to `business.data` topic:

```json
{
  "window_start": "2026-01-15T10:30:00.000000Z",
  "window_end": "2026-01-15T10:31:00.000000Z",
  "topic": "traffic.sensors",
  "district": "3",
  "avg_value": 58.7432,
  "event_count": 12
}
```

**Field Descriptions:**

- `window_start`: Start of 1-minute aggregation window
- `window_end`: End of 1-minute aggregation window
- `topic`: Source sensor domain
- `district`: Geographic district
- `avg_value`: Average measurement value across window
- `event_count`: Number of events in window

## Cleanup

Remove all containers, volumes, and data:

```bash
docker compose down -v --remove-orphans
```

