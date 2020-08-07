from flask import Flask, request, jsonify
from flask_restplus import Resource, Api, fields
import os
import mysql.connector
from jsonschema import validate, ValidationError
import json
import logging

db_pass = os.getenv('DB_PASSWD')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')

logging.basicConfig(format='%(asctime)s %(levelname)s * %(message)s')
logger = logging.getLogger('logging')

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
            logger.error(err)
            return False

    def dbQuery(self, table, columns):
            cols = ",".join(columns)
            if not  self.dbCon(db_host, db_user, db_pass, db_name):
                return False, False
            try:
                cursor = self.con.cursor()
                cursor.execute("select " + cols + " from " + table)
                resp = cursor.fetchall()
                self.con.close()
                return resp, False
            except  mysql.connector.Error as err:
                logger.warn(err)
                return False, err.errno
    
    def dbErrorMsg(self, errno):
        errdict = {
            1146 : "table doesn't exist",
            1054 : "unknown column"
        }
        if errno in errdict:
            return errdict[errno]
        else:
            return "other db issue" 
               

    def formatResponse(self, response, columns):
        json_list = []
        for row in response:
            result_json = {}
            for i in range(0, len(columns)):
                result_json[columns[i]] = row[i]
            json_list.append(result_json)
        return json_list


    @api.expect(resSchema)
    @api.response(200, 'success')
    @api.response(400, 'invalid json')
    @api.response(503, 'internal db problem')
    def post(self):
        req = request.get_json(force=True)
        try:
            validate(instance=req, schema=self.selectSchema)
        except ValidationError as err:
            return json.loads('{"message" : "json error","cause" : "' + str(err.message) +'"}'), 400
            
        columns = req['columns']
        tableName = req['table']
        query, errresp = self.dbQuery(tableName, columns)
        if query == False and errresp:
            return json.loads('{"message" : "database error","cause" : "' + self.dbErrorMsg(errresp) + '"}'), 404
        elif query == False and errresp == False:
            return json.loads('{"message" : "database error","cause" : "database is down"}'), 503
        else:
            result = self.formatResponse(query, columns)
            return result, 200


api.add_resource(GetDataFromTable, '/getdata')
api.add_resource(ListTables, '/tb')


if __name__ == '__main__':
    app.run(debug=True)
