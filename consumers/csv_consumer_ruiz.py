import os
import json
import matplotlib.pyplot as plt
import pandas as pd
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv
from collections import defaultdict

# Set up logging
log_file = "C:/Users/19564/Documents/SandraRuizPro6/buzzline-06-sruiz/logs/project_log.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # To log to terminal
        logging.FileHandler(log_file)  # To log to a file
    ]
)

# Load environment variables
load_dotenv()

# Kafka Configuration
TOPIC = os.getenv("CSV_TOPIC", "csv_data")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Aggregation Variables
age_data = defaultdict(list)
sex_counts = defaultdict(int)
job_counts = defaultdict(lambda: defaultdict(int))

# Message counter
message_count = 0

# Process Messages
for message in consumer:
    data = message.value
    sex = data["sex"]
    age = data["age"]
    job = data["job"]

    if age is not None:
        age_data[sex].append(age)
    sex_counts[sex] += 1
    job_counts[sex][job] += 1

    # Log consumed message
    logging.info(f"Consumed message: {message.value}")

    # Increment message counter
    message_count += 1

    # Stop after 5 messages for testing (you can change this limit)
    if message_count >= 1000:  # Process all messages (remove limit if needed)
        break

# Compute Average Age by Sex
average_ages = {sex: sum(ages) / len(ages) for sex, ages in age_data.items()}

# Display Results in Terminal and Log to File
df_age = pd.DataFrame(list(average_ages.items()), columns=["Sex", "Average Age"])
df_sex_counts = pd.DataFrame(list(sex_counts.items()), columns=["Sex", "Count"])
df_jobs = pd.DataFrame(
    [(sex, job, count) for sex, jobs in job_counts.items() for job, count in jobs.items()],
    columns=["Sex", "Job Title", "Count"]
)

# Log results
logging.info("\nAverage Age by Sex:\n" + df_age.to_string(index=False))
logging.info("\nNumber of Individuals by Sex:\n" + df_sex_counts.to_string(index=False))
logging.info("\nTop 5 Jobs by Sex:\n" + df_jobs.to_string(index=False))

# Log total messages consumed
logging.info(f"Total messages processed: {message_count}")
logging.info("Done processing!")

# Plot Average Age by Sex
plt.figure(figsize=(10, 5))
plt.bar(df_age["Sex"], df_age["Average Age"], color=["blue", "pink"])
plt.xlabel("Sex")
plt.ylabel("Average Age")
plt.title("Average Age by Sex")
plt.tight_layout()  # Makes sure labels are not cut off
plt.show()
plt.savefig("output_age_by_sex.png")

# Plot Count by Sex
plt.figure(figsize=(10, 5))
plt.bar(df_sex_counts["Sex"], df_sex_counts["Count"], color=["blue", "pink"])
plt.xlabel("Sex")
plt.ylabel("Count")
plt.title("Number of Individuals by Sex")
plt.tight_layout()
plt.show()
plt.savefig("output_count_by_sex.png")

# Plot Top 100 Occupations by Sex
top_jobs = df_jobs.groupby("Sex").apply(lambda x: x.nlargest(10, "Count"))
top_jobs.pivot(index="Job Title", columns="Sex", values="Count").plot(kind="bar", figsize=(12, 6))
plt.title("Top 10 Jobs by Sex")
plt.xlabel("Job Title")
plt.ylabel("Count")
plt.tight_layout()
plt.show()
plt.savefig("output_top_10_jobs_by_sex.png")


# Close Kafka Consumer
consumer.close()

# Ensure this script runs only when executed directly (not when imported as a module)
if __name__ == "__main__":
    pass




