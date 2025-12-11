from pymongo import MongoClient
from methods.config import MONGO_URI, DB_NAME, RAW_COLLECTION, REPORT_COLLECTION, ALERT_COLLECTION

mongo_client = None
db = None
raw_logs = None
reports = None
alerts = None

def connect_to_database():
    global mongo_client, db, raw_logs, reports, alerts
    
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        raw_logs = db[RAW_COLLECTION]
        reports = db[REPORT_COLLECTION]
        alerts = db[ALERT_COLLECTION]
        print("Successfully connected to MongoDB.")
        return True
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False

def get_raw_logs():
    return raw_logs

def get_reports():
    return reports

def get_alerts():
    """Get the alerts collection."""
    return alerts
