import os
from flask import Flask

app = Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
