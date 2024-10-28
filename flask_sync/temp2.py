import datetime
from threading import Thread

from flask import Flask, jsonify, request
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from elasticsearch import Elasticsearch, helpers

app = Flask(__name__)

# Configuration for MySQL and Elasticsearch
mysql_settings = {
    "host": "localhost",
    "user": "root",
    "passwd": "root_password",
    "database": "product"
}
es = Elasticsearch(["http://localhost:9200"])

# Control variables
sync_running = False
stream = None
batch_data = []
batch_size = 2  # Batch size for sending to Elasticsearch

# Function to synchronize CDC
def cdc_sync():
    global sync_running, stream, batch_data

    sync_running = True
    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=100,
        blocking=True,
        resume_stream=True,
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent]
    )

    try:
        for event in stream:
            # Process events from the binlog
            batch_data.extend(create_bulk_action(event))

            # Send batch when the size limit is reached
            if len(batch_data) >= batch_size:
                success, failed = helpers.bulk(es, batch_data)
                if not success:
                    print('A document failed:', failed)  # Log failed actions
                print("Successfully bulked to Elasticsearch")
                batch_data = []  # Clear the batch after sending

            # Check if synchronization has been stopped
            if not sync_running:
                break

        # Send any remaining data in the batch
        if batch_data:
            success, failed = helpers.bulk(es, batch_data)
            if not success:
                print('A document failed:', failed)
            print("Successfully bulked remaining data to Elasticsearch")

    finally:
        stream.close()  # Ensure stream is closed
        sync_running = False

# Create actions for Elasticsearch
def create_bulk_action(event):
    extracted_collection = []
    for row in event.rows:
        if event.schema == "product":
            if "id" in row["values"]:
                if isinstance(event, WriteRowsEvent):
                    extracted = {
                        "_op_type": "create",
                        "_index": event.table+"3",
                        "_id": row["values"]["id"],
                        "_source": row["values"]
                    }
                    print("Inserting:", row['values'])
                elif isinstance(event, UpdateRowsEvent):
                    extracted = {
                        "_op_type": "update",
                        "_index": event.table+"3",
                        "_id": row["after-values"]["id"],
                        "doc": row["after-values"]
                    }
                    print("Updating:", row['values'])
                elif isinstance(event, DeleteRowsEvent):
                    extracted = {
                        "_op_type": "delete",
                        "_index": event.table+"3",
                        "_id": row["values"]["id"]
                    }
                    print("Deleting:", row['values'])
                extracted_collection.append(extracted)
    return extracted_collection

# Function to test bulk indexing
def test_bulk_index():
    # Prepare a list of documents to index
    actions = [
        {"_index": "test", "_id": "4", "_source": {"title": "Document 1", "description": "This is the first document", "date": "2024-10-26", "tags": ["sample"], "views": 10, "published": True}},
        {"_index": "test", "_id": "5", "_source": {"title": "Document 2", "description": "This is the second document", "date": "2024-10-26", "tags": ["example"], "views": 20, "published": False}},
        {"_index": "test", "_id": "6", "_source": {"title": "Document 3", "description": "This is the third document", "date": "2024-10-26", "tags": ["elasticsearch"], "views": 30, "published": True}}
    ]

    # Execute the bulk operation
    success, failed = helpers.bulk(es, actions)
    print(f'Successfully indexed {success} actions. Failed: {failed}.')

# Endpoint to start the sync process
@app.route('/start-sync', methods=['GET'])
def start_sync():
    global sync_running
    if not sync_running:
        # test_bulk_index()  # You can call this separately or as needed
        cdc_thread = Thread(target=cdc_sync)
        cdc_thread.start()  # Start the CDC sync in a separate thread
        return jsonify({"status": "Sync started"}), 200
    return jsonify({"status": "Sync is already running"}), 400

# Endpoint to stop the sync
@app.route('/stop-sync', methods=['GET'])
def stop_sync():
    global sync_running
    if sync_running:
        sync_running = False
        return jsonify({"status": "Sync stopped"}), 200
    return jsonify({"status": "Sync is not running"}), 400

# Endpoint to check sync status
@app.route('/sync-status', methods=['GET'])
def sync_status():
    return jsonify({"sync_running": sync_running}), 200

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)
