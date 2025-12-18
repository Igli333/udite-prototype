from flask import Flask
import sys

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World! This is the Data Synchronizer of UDiTE\n'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)
