import os
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Postgres:
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
        password = os.getenv('POSTGRES_PASSWORD')
        host = os.getenv('POSTGRES_HOST')

        self.engine = create_engine(
            f"postgresql://postgres:{password}@{host}/postgres",
            pool_pre_ping=True
        )
        self.session = sessionmaker(bind=self.engine)()
