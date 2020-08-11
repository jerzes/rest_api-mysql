from flask import Flask, request, jsonify
from flask_restplus import Resource, Api, fields
from jsonschema import validate, ValidationError
from apiv1 import blueprint as apiv1
import mysql.connector
import logging
import json
import os




logging.basicConfig(format='%(asctime)s %(levelname)s * %(message)s')
logger = logging.getLogger('logging')

app = Flask(__name__)

app.register_blueprint(apiv1, url_prefix='')

app.run(debug=True)


if __name__ == '__main__':
    app.run(debug=True)
