from flask import Blueprint
from flask_restplus import Api
from apis.namespace1 import api as ns1


blueprint = Blueprint('api', __name__, url_prefix='/api/')

api = Api(blueprint)

api.add_namespace(ns1)
