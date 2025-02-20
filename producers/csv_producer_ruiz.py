import os
import json
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime
import logging

# Load environment variables
load_dotenv()

# Kafka Configuration
TOPIC = os.getenv("CSV_TOPIC", "csv_data")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Set up logging
log_file = r"C:\Users\19564\Documents\SandraRuizPro6\buzzline-06-sruiz\logs\project_log.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)  # Ensure log directory exists

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

logger.info(f"Kafka topic: {TOPIC}")
logger.info(f"Kafka bootstrap servers: {BOOTSTRAP_SERVERS}")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Read CSV File
csv_file = r"C:\Users\19564\Documents\SandraRuizPro6\buzzline-06-sruiz\data\people1000.csv"

try:
    df = pd.read_csv(csv_file, encoding="utf-8")
    print(f"✅ Successfully loaded CSV file: {csv_file}")
    logger.info(f"✅ Successfully loaded CSV file: {csv_file}")
except Exception as e:
    print(f"❌ Error reading CSV file: {e}")
    logger.error(f"❌ Error reading CSV file: {e}")
    exit(1)  # Stop execution if file reading fails

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
        "sex": row.get("Sex", "Unknown"),
        "age": calculate_age(row.get("Date of birth", "")),
        "job": row.get("Job Title", "Unknown")
    }
    
    producer.send(TOPIC, value=message)

    # Print message to terminal
    print(f"📤 Sent message: {message}")

    # Log message to file
    logger.info(f"📤 Sent message to Kafka: {message}")

print("✅ CSV data streaming to Kafka completed.")
logger.info("✅ CSV data streaming to Kafka completed.")

producer.flush()
producer.close()


