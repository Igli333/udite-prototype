CREATE TABLE sensor_data
(
    sensor_id   TEXT             NOT NULL,
    timestamp   TIMESTAMPTZ      NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    unit        TEXT             NOT NULL,
    location    geometry(Point, 4326) NOT NULL,
    metadata    jsonb,
    district_id TEXT             NOT NULL REFERENCES districts (id),
    system_id   TEXT             NOT NULL REFERENCES systrems (id),
    PRIMARY KEY (system_id, sensor_id, timestamp)
);

SELECT create_hypertable(
               'sensor_data',
               'timestamp',
               chunk_time_interval = > INTERVAL '1 day',
               if_not_exists = > TRUE
       );

CREATE INDEX sensor_data_system_district_ts_idx
    ON sensor_data (system_id, district_id, timestamp DESC);

-- Per-sensor history
CREATE INDEX sensor_data_sensor_ts_idx
    ON sensor_data (sensor_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS sensor_data_latest_idx
ON sensor_data (district_id, system_id, sensor_id, timestamp DESC);

-- Spatial filtering
CREATE INDEX sensor_data_location_idx
    ON sensor_data
    USING GIST (location);

ALTER TABLE sensor_data
    SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC',
    timescaledb.compress_segmentby = 'sensor_id'
    );

SELECT add_compression_policy(
               'sensor_data',
               INTERVAL '7 days'
       );

SELECT add_retention_policy(
               'sensor_data',
               INTERVAL '6 months'
       );