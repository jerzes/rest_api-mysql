from flask import Flask, request, jsonify
from flask_restplus import Resource, Api, fields
import os
import mysql.connector
from jsonschema import validate, ValidationError
import json

db_pass = os.getenv('DB_PASSWD')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')

app = Flask(__name__)
api = Api(app,version='0.2', title='mysqlRESTapi', description='MySQL REST API')

class ListTables(Resource):
    def get(self):
        return {'hello': 'world'}

@api.route('/getdata')
class GetDataFromTable(Resource):
    selectSchema = {
      "type": "object",
      "properties" : {
          "table"   : {"type" : "string"},
          "columns" : {"type" : "array"},
          "where"   : {"type" : "string"},
          },
      "required" : ["table", "columns"],
      "additionalProperties": False
      }
 
    resSchema = api.model('Resource',
            {
          "table"   : fields.String,
          "columns" : fields.List(fields.String),
          "where"   : fields.String,
          })

    def dbCon(self, host, user, passwd, db):
        try:
            self.con = mysql.connector.connect(host=host, database=db, user=user, password=passwd, auth_plugin='mysql_native_password')
            return True

        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            error = '{"error": ' + '"' + str(err) + '"}'
            return json.loads(error), 503

    def dbQuery(self, table, columns):
            cols = ",".join(columns)
            self.dbCon(db_host, db_user, db_pass, db_name)
            try:
                cursor = self.con.cursor()
                cursor.execute("select " + cols + " from " + table)
                return cursor.fetchall()
            except  mysql.connector.Error as err:
                error = '{"error": ' + '"' + str(err) + '"}'
                return json.loads(error), 400
            finally:
                self.con.close()

    @api.expect(resSchema)
    @api.response(200, 'success')
    @api.response(400, 'invalid json')
    @api.response(503, 'internal db problem')
    def post(self):
        req = request.get_json(force=True)
        try:
            validate(instance=req, schema=self.selectSchema)
        except ValidationError as err:
            return json.loads('{"error" : "True","msg" : "not valid json"}')
        columns = req['columns']
        tableName = req['table']
        result = self.dbQuery(tableName, columns)
        return result, 200


api.add_resource(GetDataFromTable, '/getdata')
api.add_resource(ListTables, '/tb')


if __name__ == '__main__':
    app.run(debug=True)
