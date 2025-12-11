import threading
from methods.database import connect_to_database
from methods.mqtt_handler import create_mqtt_client, connect_and_subscribe
from methods.reporter import generate_minute_reports


def main():
    
    if not connect_to_database():
        print("Failed to connect to database. Exiting.")
        return
    
    client = create_mqtt_client()
    
    if not connect_and_subscribe(client):
        print("Failed to connect to MQTT broker. Exiting.")
        return
    
    reporter_thread = threading.Thread(target=generate_minute_reports, daemon=True, name='MinuteReporter')
    reporter_thread.start()
    
    print("Subscriber is running. Press Ctrl+C to stop.")
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nShutting down...")
        client.loop_stop()
        client.disconnect()
        print("Disconnected from MQTT broker.")

if __name__ == "__main__":
    main()
