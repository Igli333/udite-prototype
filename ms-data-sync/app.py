from flask import Flask
from app.controller import sensors
import app.repository as repository

app = Flask(__name__)
app.register_blueprint(sensors)
pg_db = repository.PostGIS()
# influx_db = repository.InfluxDB()


@app.route('/')
def hello_world():
    return 'Hello World! This is the Data Synchronizer of UDiTE\n'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)
