from flask import Flask, jsonify, request
from flask_cors import CORS
from py2neo import Graph, Node, NodeMatcher


app = Flask(__name__)
CORS(app)
app = Flask(__name__)


@app.route('/')
def hello_world():  # put application's code here
    return jsonify({'msg': 'Hello w0rld'})


if __name__ == '__main__':
    app.run(debug=True)
