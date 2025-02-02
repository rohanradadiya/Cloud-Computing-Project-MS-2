from google.cloud import pubsub_v1
import mysql.connector
import json
import os

# Set up MySQL connection
db = mysql.connector.connect(
    host="34.130.238.55",  # e.g., '127.0.0.1' or your Google Cloud SQL IP
    user="root",            # your MySQL username
    password="mysql-password",
    database="DatasetDB"    # your database name
)

cursor = db.cursor()

# Set up Pub/Sub subscriber
project_id = "sodium-sublime-448820-j1"
subscription_name = "csvRecordsSQL-subscription"  # Change this as needed
subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"

subscriber = pubsub_v1.SubscriberClient()

def callback(message):
    print(f"Received message: {message.data}")
    try:
        # Parse the message data (which is a JSON object)
        record = json.loads(message.data.decode('utf-8'))
        
        # Extract values from the record
        label = record.get("pedestrianLocationX_TopLeft", "")  # Adjust based on your CSV structure
        filename = record.get("filename", "")  # Replace with actual field if available
        additional_info = json.dumps(record)  # Store the whole record as text
        
        # Insert into the database
        cursor.execute(
            "INSERT INTO CsvRecords (label, filename, additional_info) VALUES (%s, %s, %s)",
            (label, filename, additional_info)
        )
        db.commit()
        print(f"Inserted record: {record}")
        
    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        message.ack()

# Subscribe to the topic
subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}...")

# Keep the main thread alive to continue receiving messages
try:
    # Keep the main thread alive to listen for messages indefinitely
    while True:
        pass
except KeyboardInterrupt:
    print("Subscriber stopped.")
finally:
    # Cleanup the MySQL connection when done
    db.close()
    print("MySQL connection closed.")
