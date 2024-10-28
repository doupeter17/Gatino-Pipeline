from flask import Flask
import mysql.connector
import json
import datetime
import decimal
from mysql.connector import Error
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent
from elasticsearch import Elasticsearch, helpers

app = Flask(__name__)



MYSQL_SETTINGS = {"host": "127.0.0.1", "port": 3306, "user": "root", "passwd": "root_password", "db":"product"}
class MysqlEs:
    def __init__(self):
        self.es = None
        self.mysql_conn = False
        self.es_conn = False
        self.es_data = []
        self.mysql_connection()
        self.es_connection()
        self.logStream = BinLogStreamReader(
            connection_settings=MYSQL_SETTINGS,
            server_id=3,
            blocking=True,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, XidEvent],
        )
    def mysql_connection(self):
        try:
            # Establish a connection
            connection = mysql.connector.connect(
                host='127.0.0.1',
                port='3306',# Replace with your host, e.g., '127.0.0.1'
                user='root',  # Replace with your MySQL username
                password='root_password',  # Replace with your MySQL password
                database='product'  # Optional: replace with your database name
            )

            if connection.is_connected():
                self.mysql_conn = True
                print("Successfully connected to MySQL database")
                # Get and display server information
                db_info = connection.get_server_info()
                print("Server version:", db_info)

                # Optionally, fetch database name
                cursor = connection.cursor()
                cursor.execute("SELECT DATABASE();")
                db_name = cursor.fetchone()
                print("Connected to database:", db_name)

            else:
                print("Failed to connect MySQL database.")
                self.mysql_conn = False
        except Error as e:
            print("Error while connecting to MySQL", e)

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")

    def es_connection(self):
        self.es = Elasticsearch("http://127.0.0.1:9200/")
        if(self.es.ping() == True):
            print("Connect to ElasticSearch successfully")
            self.es_conn = True
        else:
            print("Failed connect to ElasticSearch!!!")
            self.es_conn = False

    def get_event(self):
        extracted_collection = []
        for event in self.logStream:
            if isinstance(event, XidEvent):
                continue
            for row in event.rows:
                if(event.schema == "product"):
                    if("id" in row["values"]):
                        if isinstance(event, WriteRowsEvent):
                            extracted = {
                                "_op_type": "index",
                                "_index": event.table,
                                "_source": row["values"]
                            }
                            print(row['values'])
                        elif isinstance(event, UpdateRowsEvent):
                            extracted = {
                                "_op_type": "update",
                                "_index": event.table,
                                "_id": row["after-values"]["id"],
                                "doc": row["after-values"]
                            }
                            print(row['values'])
                        if isinstance(event, DeleteRowsEvent):
                            extracted = {
                                "_op_type": "delete",
                                "_index": event.table,
                                "_id": row["values"]["id"]
                            }
                            print(row['values'])
                        extracted_collection.append(extracted)
        self.logStream.close()
        print('Info: Mysql connection closed successfully after reading all binlog events.')
        return extracted_collection


    def send_to_es(self, json):
        helpers.bulk(self.es, json)
        print("success")


@app.route('/')
def index():
    mysql_es = MysqlEs()
    event = mysql_es.get_event()
    mysql_es.send_to_es(event)
    return "Flask app is running! Data sync is scheduled every minute."
if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000)

