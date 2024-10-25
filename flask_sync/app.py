from flask import Flask
import mysql.connector
import json
from mysql.connector import Error
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent
from elasticsearch import Elasticsearch
app = Flask(__name__)



MYSQL_SETTINGS = {"host": "127.0.0.1", "port": 3306, "user": "root", "passwd": "root_password", "db":"product"}
class MysqlEs:
    def __init__(self):
        self.mysql_conn = False
        self.es_conn = False
        self.extracted_collection = []
        self.mysql_connection()
        self.es_connection()
        self.logStream = BinLogStreamReader(
            connection_settings=MYSQL_SETTINGS,
            server_id=3,
            blocking=False,
            log_file='binlog.000001',
            log_pos=0,
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
        es = Elasticsearch("http://127.0.0.1:9200/")
        if(es.ping() == True):
            print("Connect to ElasticSearch successfully")
            self.es_conn = True
        else:
            print("Failed connect to ElasticSearch!!!")
            self.es_conn = False

    def get_event(self):
        extracted_collection = []
        for event in self.logStream:
            # event.dump()
            if isinstance(event, XidEvent):
                # yield extracted_collection
                #
                # extracted_collection = []
                continue
        #
            for row in event.rows:
        #         if isinstance(event, DeleteRowsEvent):
        #             extracted = {
        #                 'index': event.table,
        #                 'id': row['values'][event.primary_key],
        #                 'action': 'delete'
        #             }
        #         elif isinstance(event, UpdateRowsEvent):
        #             extracted = {
        #                 'index': event.table,
        #                 'id': row['after_values'][event.primary_key],
        #                 'action': 'update',
        #                 'doc': {k: v for k, v in row['after_values'].items() if k != event.primary_key}
        #             }
        #         if isinstance(event, WriteRowsEvent):
        #             extracted = {
        #                 'index': event.table,
        #                 'id': row['values'][event.primary_key],
        #                 'action': 'create',
        #                 'doc': {k: v for k, v in row['values'].items() if k != event.primary_key}
        #             }
                print(event.table)
                print(row["values"])
        #         extracted_collection.append(extracted)
        # self.extracted_collection = extracted_collection
        # self.logStream.close()
        # print('Info: Mysql connection closed successfully after reading all binlog events.')


@app.route('/')
def index():
    mysql_es = MysqlEs()
    mysql_es.get_event()
    return "Flask app is running! Data sync is scheduled every minute."


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000)

