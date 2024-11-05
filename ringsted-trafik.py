import paho.mqtt.client as mqtt
import time
import random
from datetime import datetime, timedelta
import json
 
# Define MQTT broker details
broker_address = "localhost"
port = 1883
summer_dress_topic = "summerdress"
car_crash_topic = "carcrash"
batch_topic = "batch"
 
# Create MQTT client
client = mqtt.Client()
 
# Connect to the broker
client.connect(broker_address, port=port)
 
# Set up batch data stores
batch_summer_dresses = 0
batch_car_crashes = 0
batch_start = datetime.now()
 
try:
    while True:
        # Get random numbers
        sleep_time = random.randint(1, 10)
        summer_dresses = random.randint(0, 17)
        car_crashes = random.randint(0,3)
 
        # Add to batch data
        batch_summer_dresses += summer_dresses
        batch_car_crashes += car_crashes
 
        # Get timestamp
        timestamp = datetime.timestamp(datetime.now())
 
        # Publish message to the topic
        data = {
            "sundresses":summer_dresses,
            "timestamp":timestamp
        }
 
        message = json.dumps(data)
 
        client.publish(summer_dress_topic, message)
 
        data = {
            "car_crashes":car_crashes,
            "timestamp":timestamp
        }
 
        message = json.dumps(data)
 
        client.publish(car_crash_topic, message)
 
        # Send batch data if it has been ~5 minutes
        if datetime.now() - batch_start >= timedelta(minutes=5):
 
            print(f"Batch time: {batch_start}")
            print(f"Current time: {datetime.now()}")
            print(f"Time difference: {datetime.now() - batch_start}")
 
            data = {
                "sundresses":batch_summer_dresses,
                "car_crashes":batch_car_crashes,
            }
 
            message = json.dumps(data)
 
            client.publish(batch_topic, message)
 
            # Reset batch data
            batch_start = datetime.now()
            batch_summer_dresses = 0
            batch_car_crashes = 0
 
            print("Batch data sent")
 
        # Wait before sending the next message
        time.sleep(sleep_time)
except KeyboardInterrupt:
    print("Thanks for using our MQTT nonsense!")
 
# Disconnect the client
client.disconnect()