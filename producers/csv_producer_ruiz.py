import os
import json
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Kafka Configuration
TOPIC = os.getenv("CSV_TOPIC", "csv_data")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

csv_file = r"C:\Users\19564\Documents\SandraRuizPro6\buzzline-06-sruiz\data\people1000.csv"
df = pd.read_csv(csv_file, encoding="utf-8")



# Function to calculate age
def calculate_age(birth_date):
    try:
        birth_year = pd.to_datetime(birth_date, errors="coerce").year
        return datetime.now().year - birth_year if pd.notna(birth_year) else None
    except Exception:
        return None


# Stream Data to Kafka
for _, row in df.iterrows():
    message = {
     message = {
    "sex": row.get("Sex", "Unknown"),
    "age": calculate_age(row.get("Date of birth", "")),
    "job": row.get("Job Title", "Unknown")
}

    }
    producer.send(TOPIC, message)

print("CSV data streaming to Kafka...")
producer.flush()
producer.close()
