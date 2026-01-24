from flask import Flask
from app.controller import sensors, simulator

app = Flask(__name__)

app.register_blueprint(sensors)
app.register_blueprint(simulator)


@app.route('/')
def hello_world():
    return 'Hello World! This is the Data Synchronizer of UDiTE\n'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)
