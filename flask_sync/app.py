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
            resume_stream=True,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
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
            event.dump()
            extracted_collection.extend(self.event_to_es(event))
            # if(len(extracted_collection) >= 6):
            print(extracted_collection)
            self.send_to_es(extracted_collection)
            extracted_collection = []
        self.logStream.close()
        print('Info: Mysql connection closed successfully after reading all binlog events.')
        return extracted_collection

    def event_to_es(self, event):
        rows = []
        for row in event.rows:
            if (event.schema == "product"):
                if ("id" in row["values"]):
                    if isinstance(event, WriteRowsEvent):
                        extracted = [
                            {"index": {"_index": event.table, "_id": row["values"]["id"]}}
                        ]
                        extracted.append(row["values"])
                        print(row['values'])
                    elif isinstance(event, UpdateRowsEvent):
                        extracted = [
                            {"update": {"_index": event.table, "_id": row["values"]["id"]}}
                        ]
                        extracted.append(
                            {
                               "doc": row["values"]
                            }
                        )
                    if isinstance(event, DeleteRowsEvent):
                        extracted = {"delete": {"_index": event.table, "_id": row["values"]["id"]}},
                        print(row['values'])
                    rows.extend(extracted)
        return rows
    def send_to_es(self, data):

        # data2 = [
        #     {"create": {"_index": "product", "_id": 3030}},
        #     {
        #         "id":"1726",
        #         "productName": "3123",
        #         "productCode": "asu3s",
        #         "productStatusFK": 1,
        #         "active": 0,
        #         "updatedAt": datetime.datetime(2000,1,1)
        #     },
        #     {"create": {"_index": "product", "_id": 3031}},
        #     {
        #         "id": "1726",
        #         "productName": "3123",
        #         "productCode": "asu3s",
        #         "productStatusFK": 1,
        #         "active": 0,
        #         "updatedAt": datetime.datetime(2000,1,1)
        #     },
        #     {"create": {"_index": "product", "_id": 3032}},
        #     {
        #         "id": "1726",
        #         "productName": "31sss23",
        #         "productCode": "asssu3s",
        #         "productStatusFK": 1,
        #         "active": 0,
        #         "updatedAt": datetime.datetime(2000,1,1)
        #     }
        # ]

        # self.es.indices.delete(index='product', ignore_unavailable=True)
        # self.es.indices.create(index='product')
        response =self.es.bulk(
            operations=data
        )

        print(response.body)
        print(self.es.count(index='product'))

@app.route('/')
def index():
    mysql_es = MysqlEs()
    event = mysql_es.get_event()
    mysql_es.send_to_es(event)
    return "Flask app is running! Data sync is scheduled every minute."
if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000)

