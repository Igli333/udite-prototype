import threading

from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry

class PostGIS:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, app, db_uri):
        self.db = SQLAlchemy()
        if app:
            if db_uri:
                app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
            app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
            self.db.init_app(app)

