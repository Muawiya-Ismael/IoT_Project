import paho.mqtt.client as mqtt
import time
import json
import random

# --- MQTT Configuration ---
# Use 'localhost' if Mosquitto is running on the same machine, otherwise use its IP address
MQTT_BROKER = "localhost" 
MQTT_PORT = 1883
# The main topic for bin sensor data
MQTT_TOPIC = "/smartbin/data" 
CLIENT_ID = f'python-mqtt-publisher-{random.randint(0, 1000)}'
DATA_FILE = "data.json"

# --- Callback Functions ---

def on_connect(client, userdata, flags, rc):
    """Callback function executed when the client connects to the broker."""
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {rc}")

def on_publish(client, userdata, mid):
    """Callback function executed when a message has been published."""
    # print(f"Message ID: {mid} published.")
    pass

# --- Main Logic ---

def read_data_file(file_path):
    """Reads all sensor data from the JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: Data file '{file_path}' not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{file_path}'. Check file format.")
        return []

def publish_data_stream(client, data):
    """Publishes each data point from the list to the MQTT topic."""
    if not data:
        print("No data to publish. Exiting.")
        return

    print(f"--- Starting data stream on topic: {MQTT_TOPIC} ---")
    
    # Iterate through each data point in the list
    for i, reading in enumerate(data):
        # Update timestamp to current time for real-time readings
        reading['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        # Convert the Python dictionary into a JSON string payload
        payload = json.dumps(reading)
        
        # Publish the message. QoS=1 ensures the message is delivered at least once.
        result = client.publish(MQTT_TOPIC, payload, qos=1)
        
        # Check the result of the publish operation
        status = result[0]
        if status == mqtt.MQTT_ERR_SUCCESS:
            print(f"Sent {i+1}/{len(data)}: Bin {reading['bin_id']} @ {reading['capacity_percent']}%")
        else:
            print(f"Failed to send message {i+1} to topic {MQTT_TOPIC}. Status: {status}")
            
        # Wait for 3 seconds before publishing the next reading (simulating sensor interval)
        time.sleep(3) 

def run_publisher():
    """Sets up the client, connects, and starts the publishing loop."""
    client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)
    
    # Assign callback functions
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    # Connect to the broker
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"Could not connect to broker at {MQTT_BROKER}:{MQTT_PORT}. Error: {e}")
        return

    # Start the network loop to process callbacks and handle messages
    client.loop_start() 
    
    # Get all the data
    sensor_data = read_data_file(DATA_FILE)
    
    # Publish the data stream
    publish_data_stream(client, sensor_data)
    
    # Stop the network loop and disconnect after publishing all data
    print("--- Data stream finished. Disconnecting. ---")
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    run_publisher()