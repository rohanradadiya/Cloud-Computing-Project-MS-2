from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import json
import os 
import random
import numpy as np                      # pip install numpy    ##to install
import time

# Set the path for the service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\rohan\\Documents\\CCMilestone2\\sodium-sublime-448820-j1-c4c9c13e145e.json"

# Set the project_id with your project ID
project_id = "sodium-sublime-448820-j1"
topic_name = "smartMeterReadings"   # Topic name for the Smart Meter readings

# Create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# Device normal distributions profile used to generate random data
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091) },
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341) },
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201) },
}

profileNames = ["boston", "denver", "losang"]

ID = np.random.randint(0, 10000000)

while True:
    # Randomly select a profile name
    profile_name = profileNames[random.randint(0, 2)]
    profile = DEVICE_PROFILES[profile_name]
    
    # Generate random values within a normal distribution of the value
    temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))
    
    # Create a dictionary to store the message
    msg = {"ID": ID, "time": int(time.time()), "profile_name": profile_name, "temperature": temp, "humidity": humd, "pressure": pres}
    ID = ID + 1
    
    # Randomly eliminate some measurements
    if random.randrange(0, 10) < 1:
        msg['temperature'] = None
    if random.randrange(0, 10) < 1:
        msg['humidity'] = None
    if random.randrange(0, 10) < 1:
        msg['pressure'] = None
                
    # Serialize the message to JSON format
    record_value = json.dumps(msg).encode('utf-8')
    
    try:    
        # Publish the message to the topic
        future = publisher.publish(topic_path, record_value)
        
        # Ensure that the publishing has been completed successfully
        future.result()
        print(f"The message {msg} has been published successfully.")
    except Exception as e:
        print(f"Failed to publish the message: {e}")
    
    # Wait for 0.5 second before sending the next message
    time.sleep(0.5)
