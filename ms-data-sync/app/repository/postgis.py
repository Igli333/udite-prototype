import threading

from ..model import SensorData
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .postgis_base import Base


class PostGIS:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        password = "password"
        self.engine = create_engine(f"postgresql://postgres:{password}@localhost:5432/postgres")
        self.initialize_schema()
        self.session = sessionmaker(bind=self.engine)()

    def initialize_schema(self):
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.districts
                    (
                        id   TEXT PRIMARY KEY,
                        name TEXT                         NOT NULL,
                        geom geometry(MultiPolygon, 4326) NOT NULL
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.emergency_resources_data
                    (
                        sensor_id   UUID                  NOT NULL,
                        timestamp   TIMESTAMP             NOT NULL,
                        value       FLOAT                 NOT NULL,
                        unit        TEXT                  NOT NULL,
                        location    geometry(Point, 4326) NOT NULL,
                        district_id TEXT                  NOT NULL,
                        system      TEXT                  NOT NULL,
                        type        TEXT                  NOT NULL,
                        PRIMARY KEY (sensor_id, timestamp, district_id)
                    ) PARTITION BY LIST (district_id);
                    """
                ))

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.green_infrastructure_data
                    (
                        sensor_id UUID                  NOT NULL,
                        timestamp TIMESTAMP             NOT NULL,
                        value     FLOAT                 NOT NULL,
                        unit      TEXT                  NOT NULL,
                        location  geometry(Point, 4326) NOT NULL,
                        district  TEXT                  NOT NULL,
                        system    TEXT                  NOT NULL,
                        PRIMARY KEY (sensor_id, timestamp, district)
                    ) PARTITION BY LIST (district);
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.telecommunication_data
                    (
                        sensor_id UUID                  NOT NULL,
                        timestamp TIMESTAMP             NOT NULL,
                        value     FLOAT                 NOT NULL,
                        unit      TEXT                  NOT NULL,
                        location  geometry(Point, 4326) NOT NULL,
                        district  TEXT                  NOT NULL,
                        system    TEXT                  NOT NULL,
                        PRIMARY KEY (sensor_id, timestamp, district)
                    ) PARTITION BY LIST (district);
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.traffic_data
                    (
                        sensor_id UUID                  NOT NULL,
                        timestamp TIMESTAMP             NOT NULL,
                        value     FLOAT                 NOT NULL,
                        unit      TEXT                  NOT NULL,
                        location  geometry(Point, 4326) NOT NULL,
                        district  TEXT                  NOT NULL,
                        system    TEXT                  NOT NULL,
                        PRIMARY KEY (sensor_id, timestamp, district)
                    ) PARTITION BY LIST (district);
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.telecom_data
                    (
                        sensor_id UUID                  NOT NULL,
                        timestamp TIMESTAMP             NOT NULL,
                        value     FLOAT                 NOT NULL,
                        unit      TEXT                  NOT NULL,
                        location  geometry(Point, 4326) NOT NULL,
                        district  TEXT                  NOT NULL,
                        system    TEXT                  NOT NULL,
                        PRIMARY KEY (sensor_id, timestamp, district)
                    ) PARTITION BY LIST (district);
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.transport_data
                    (
                        sensor_id UUID                  NOT NULL,
                        timestamp TIMESTAMP             NOT NULL,
                        value     FLOAT                 NOT NULL,
                        unit      TEXT                  NOT NULL,
                        location  geometry(Point, 4326) NOT NULL,
                        district  TEXT                  NOT NULL,
                        system    TEXT                  NOT NULL,
                        PRIMARY KEY (sensor_id, timestamp, district)
                    ) PARTITION BY LIST (district);
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.utilities_data
                    (
                        sensor_id UUID                  NOT NULL,
                        timestamp TIMESTAMP             NOT NULL,
                        value     FLOAT                 NOT NULL,
                        unit      TEXT                  NOT NULL,
                        location  geometry(Point, 4326) NOT NULL,
                        district  TEXT                  NOT NULL,
                        system    TEXT                  NOT NULL,
                        PRIMARY KEY (sensor_id, timestamp, district)
                    ) PARTITION BY LIST (district);
                    """
                )
            )
