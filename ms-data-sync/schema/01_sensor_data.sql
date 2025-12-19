CREATE TABLE IF NOT EXISTS emergency_resources_data
(
    sensor_id   UUID                  NOT NULL,
    timestamp   TIMESTAMPTZ           NOT NULL,
    value       DOUBLE PRECISION      NOT NULL,
    unit        TEXT                  NOT NULL,
    location    geometry(Point, 4326) NOT NULL,
    district_id TEXT                  NOT NULL REFERENCES districts (id),
    PRIMARY KEY (sensor_id, timestamp, district_id)
);

SELECT create_hypertable(
               'emergency_resources_data',
               'timestamp',
               chunk_time_interval => interval '1 day',
               partitioning_column => 'district_id',
               number_partitions => 64,
               if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS emergency_resources_data_location_idx
    ON emergency_resources_data USING GIST (location);

CREATE INDEX IF NOT EXISTS emergency_resources_data_district_system_ts_idx
    ON emergency_resources_data (district_id, system, timestamp DESC);

ALTER TABLE public.emergency_resources_data
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'sensor_id, district_id'
        );

-- Compress chunks older than 7 days
SELECT add_compression_policy('public.emergency_resources_data', INTERVAL '7 days');

-- Retain raw data for 6 months
SELECT add_retention_policy('public.emergency_resources_data', INTERVAL '6 months');

-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS green_infrastructure_data
(
    sensor_id   UUID                  NOT NULL,
    timestamp   TIMESTAMPTZ           NOT NULL,
    value       DOUBLE PRECISION      NOT NULL,
    unit        TEXT                  NOT NULL,
    location    geometry(Point, 4326) NOT NULL,
    district_id TEXT                  NOT NULL REFERENCES districts (id),
    PRIMARY KEY (sensor_id, timestamp, district_id)
);

SELECT create_hypertable(
               'green_infrastructure_data',
               'timestamp',
               chunk_time_interval => interval '1 day',
               partitioning_column => 'district_id',
               number_partitions => 64,
               if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS green_infrastructure_data_location_idx
    ON green_infrastructure_data USING GIST (location);

CREATE INDEX IF NOT EXISTS green_infrastructure_data_district_system_ts_idx
    ON green_infrastructure_data (district_id, system, timestamp DESC);

ALTER TABLE public.green_infrastructure_data
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'sensor_id, district_id'
        );

-- Compress chunks older than 7 days
SELECT add_compression_policy('public.green_infrastructure_data', INTERVAL '7 days');

-- Retain raw data for 6 months
SELECT add_retention_policy('public.green_infrastructure_data', INTERVAL '6 months');

----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS telecommunication_infrastructure_data
(
    sensor_id   UUID                  NOT NULL,
    timestamp   TIMESTAMPTZ           NOT NULL,
    value       DOUBLE PRECISION      NOT NULL,
    unit        TEXT                  NOT NULL,
    location    geometry(Point, 4326) NOT NULL,
    district_id TEXT                  NOT NULL REFERENCES districts (id),
    PRIMARY KEY (sensor_id, timestamp, district_id)
);

SELECT create_hypertable(
               'telecommunication_infrastructure_data',
               'timestamp',
               chunk_time_interval => interval '1 day',
               partitioning_column => 'district_id',
               number_partitions => 64,
               if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS telecommunication_infrastructure_data_location_idx
    ON telecommunication_infrastructure_data USING GIST (location);

CREATE INDEX IF NOT EXISTS telecommunication_infrastructure_data_district_system_ts_idx
    ON telecommunication_infrastructure_data (district_id, system, timestamp DESC);

ALTER TABLE public.telecommunication_infrastructure_data
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'sensor_id, district_id'
        );

-- Compress chunks older than 7 days
SELECT add_compression_policy('public.telecommunication_infrastructure_data', INTERVAL '7 days');

-- Retain raw data for 6 months
SELECT add_retention_policy('public.telecommunication_infrastructure_data', INTERVAL '6 months');

-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS traffic_data
(
    sensor_id   UUID                  NOT NULL,
    timestamp   TIMESTAMPTZ           NOT NULL,
    value       DOUBLE PRECISION      NOT NULL,
    unit        TEXT                  NOT NULL,
    location    geometry(Point, 4326) NOT NULL,
    district_id TEXT                  NOT NULL REFERENCES districts (id),
    PRIMARY KEY (sensor_id, timestamp, district_id)
);

SELECT create_hypertable(
               'traffic_data',
               'timestamp',
               chunk_time_interval => interval '1 day',
               partitioning_column => 'district_id',
               number_partitions => 64,
               if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS traffic_data_location_idx
    ON traffic_data USING GIST (location);

CREATE INDEX IF NOT EXISTS traffic_data_district_system_ts_idx
    ON traffic_data (district_id, system, timestamp DESC);

ALTER TABLE public.traffic_data
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'sensor_id, district_id'
        );

-- Compress chunks older than 7 days
SELECT add_compression_policy('public.traffic_data', INTERVAL '7 days');

-- Retain raw data for 6 months
SELECT add_retention_policy('public.traffic_data', INTERVAL '6 months');

-------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public_transport_data
(
    sensor_id   UUID                  NOT NULL,
    timestamp   TIMESTAMPTZ           NOT NULL,
    value       DOUBLE PRECISION      NOT NULL,
    unit        TEXT                  NOT NULL,
    location    geometry(Point, 4326) NOT NULL,
    district_id TEXT                  NOT NULL REFERENCES districts (id),
    PRIMARY KEY (sensor_id, timestamp, district_id)
);

SELECT create_hypertable(
               'public_transport_data',
               'timestamp',
               chunk_time_interval => interval '1 day',
               partitioning_column => 'district_id',
               number_partitions => 64,
               if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS public_transport_data_location_idx
    ON public_transport_data USING GIST (location);

CREATE INDEX IF NOT EXISTS public_transport_data_district_system_ts_idx
    ON public_transport_data (district_id, system, timestamp DESC);

ALTER TABLE public.public_transport_data
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'sensor_id, district_id'
        );

-- Compress chunks older than 7 days
SELECT add_compression_policy('public.public_transport_data', INTERVAL '7 days');

-- Retain raw data for 6 months
SELECT add_retention_policy('public.public_transport_data', INTERVAL '6 months');

-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS utilities_data
(
    sensor_id   UUID                  NOT NULL,
    timestamp   TIMESTAMPTZ           NOT NULL,
    value       DOUBLE PRECISION      NOT NULL,
    unit        TEXT                  NOT NULL,
    location    geometry(Point, 4326) NOT NULL,
    district_id TEXT                  NOT NULL REFERENCES districts (id),
    PRIMARY KEY (sensor_id, timestamp, district_id)
);

SELECT create_hypertable(
               'utilities_data',
               'timestamp',
               chunk_time_interval => interval '1 day',
               partitioning_column => 'district_id',
               number_partitions => 64,
               if_not_exists => TRUE
       );

CREATE INDEX IF NOT EXISTS utilities_data_location_idx
    ON utilities_data USING GIST (location);

CREATE INDEX IF NOT EXISTS utilities_data_district_system_ts_idx
    ON utilities_data (district_id, system, timestamp DESC);

ALTER TABLE public.utilities_data
    SET (
        timescaledb.compress,
        timescaledb.compress_orderby = 'timestamp DESC',
        timescaledb.compress_segmentby = 'sensor_id, district_id'
        );

-- Compress chunks older than 7 days
SELECT add_compression_policy('public.utilities_data', INTERVAL '7 days');

-- Retain raw data for 6 months
SELECT add_retention_policy('public.utilities_data', INTERVAL '6 months');

