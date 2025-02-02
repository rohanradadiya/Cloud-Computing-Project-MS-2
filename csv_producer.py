from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for JSON file
import json
import os
import csv
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 

# Set the project_id with your project ID
#project_id = "sodium-sublime-448820-j1"
topic_name = "csvRecordsSQL"   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}...")

# Read from Labels.csv
csv_file_path = "Labels.csv"

try:
    with open(csv_file_path, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for index, row in enumerate(reader):
            # Convert data types where necessary
            row['Timestamp'] = int(row['Timestamp'])
            row['Car1_Location_X'] = float(row['Car1_Location_X'])
            row['Car1_Location_Y'] = float(row['Car1_Location_Y'])
            row['Car1_Location_Z'] = float(row['Car1_Location_Z'])
            row['Car2_Location_X'] = float(row['Car2_Location_X'])
            row['Car2_Location_Y'] = float(row['Car2_Location_Y'])
            row['Car2_Location_Z'] = float(row['Car2_Location_Z'])
            row['pedestrianLocationX_TopLeft'] = int(row['pedestrianLocationX_TopLeft'])
            row['pedestrianLocationY_TopLeft'] = int(row['pedestrianLocationY_TopLeft'])
            row['pedestrianLocationX_BottomRight'] = int(row['pedestrianLocationX_BottomRight'])
            row['pedestrianLocationY_BottomRight'] = int(row['pedestrianLocationY_BottomRight'])

            # Serialize the message
            record_value = json.dumps(row).encode('utf-8')

            try:
                # Publish the message
                future = publisher.publish(topic_path, record_value)

                #ensure that the publishing has been completed successfully
                future.result()
                print(f"[{index+1}] Published successfully: {row}")
            except Exception as e:
                print(f"[{index+1}] Failed to publish: {e}")

            time.sleep(0.5)   # Wait for 0.5 second

except FileNotFoundError:
    print(f"CSV file '{csv_file_path}' not found.")
except Exception as e:
    print(f"Unexpected Error: {e}")
