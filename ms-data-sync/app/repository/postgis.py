import threading

from ..model import SensorData
from sqlalchemy import create_engine
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

        Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()


