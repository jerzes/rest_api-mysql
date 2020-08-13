from flask import Flask, request
from flask_restplus import Resource, fields, Namespace
from jsonschema import validate, ValidationError
import mysql.connector
import logging
import json
import os


db_pass = os.getenv('DB_PASSWD')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')

api = Namespace('api', description='Gets data from table base on json payload', path='/')

logging.basicConfig(format='%(asctime)s %(levelname)s * %(message)s')
logger = logging.getLogger('logging')

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

@api.route('/gettables')
class GetTables(Resource):
    def gettables(self):
        try:
            con = mysql.connector.connect(host=db_host, database=db_name, user=db_user, password=db_pass, auth_plugin='mysql_native_password')
            cursor = con.cursor()
            cursor.execute("show tables")
            resp = cursor.fetchall()
            return resp

        except mysql.connector.Error as err:
            logger.error(err)
            return False

    def formatResponse(self, response):
        tablist = []
        for i in response:
            table_list = '{"name": "'+ i[0] + '"}'
            tablist.append(json.loads(table_list))
        return tablist

    def get(self):
        gettab = self.gettables()
        if gettab:
            response = self.formatResponse(gettab)
            return response, 200
        else:
            return  json.loads('{"message" : "database error","cause" : "database is down"}'), 503