from flask import Flask
import mysql.connector
from mysql.connector import Error
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent
from elasticsearch import Elasticsearch as es
app = Flask(__name__)



MYSQL_SETTINGS = {"host": "127.0.0.1", "port": 3306, "user": "root", "passwd": "root_password", "db":"product"}

def mysql_connection():
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
            print("Successfully connected to MySQL database")
            # Get and display server information
            db_info = connection.get_server_info()
            print("Server version:", db_info)

            # Optionally, fetch database name
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()
            print("Connected to database:", db_name)

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=3,
        blocking=False,
        log_file='binlog.000001',
        log_pos=0,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, XidEvent],
    )

    for binlogevent in stream:
        binlogevent.dumps()

    stream.close()

@app.route('/')
def index():
    mysql_connection()
    main()
    return "Flask app is running! Data sync is scheduled every minute."


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=5000)

