import paho.mqtt.client as mqtt
from methods.config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, CLIENT_ID
from methods.data_processor_alerts_generator import process_and_store_data


def on_connect(client, userdata, flags, rc):
    """Callback function executed when the client connects to the broker."""
    if rc == 0:
        print("Connected to MQTT Broker!")
        # Subscribe to the topic upon successful connection
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}")


def on_message(client, userdata, msg):
    """Callback function executed when a message is received."""
    # The message payload is in bytes, decode it to a UTF-8 string
    payload_str = msg.payload.decode('utf-8')
    process_and_store_data(payload_str)


def create_mqtt_client():
    """Creates and configures an MQTT client."""
    client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)
    
    # Assign callback functions
    client.on_connect = on_connect
    client.on_message = on_message
    
    return client


def connect_and_subscribe(client):
    """Connects the MQTT client to the broker and starts the network loop."""
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"Could not connect to broker at {MQTT_BROKER}:{MQTT_PORT}. Error: {e}")
        return False
    
    # Start the network loop
    client.loop_start()
    return True
