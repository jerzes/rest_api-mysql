from flask import Flask, request
from flask_restful import Resource, Api
import os
import mysql.connector

db_pass = os.getenv('DB_PASS')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')

app = Flask(__name__)
api = Api(app)


class ListTables(Resource):
    def get(self):
        return {'hello': 'world'}

class GetDataFromTable(Resource):
    def dbCon(self, host, user, passwd, db):
        con = mysql.connector.connect(host=host, database=db, user=user, password=passwd)
        if con:
            return self.con
        else:
            return False

    def dbQuery(table, columns):
        cols = ",".join(columns)
        
        dbCon(db_host, db_name, db_user, db_pass)
        try:
            cursor = self.con.cursor()
            cursor.execute("select " + col + " from " + table)
            return cursor.fetchAll()
        finally:
            self.con.close()


    def post(self):
        req = request.json
        columns = req['columns']
        tableName = req['table']
        result = dbQuery(tableName, columns)
        return result


api.add_resource(GetDataFromTable, '/getdata')
api.add_resource(ListTables, '/tb')


if __name__ == '__main__':
    app.run(debug=True)
