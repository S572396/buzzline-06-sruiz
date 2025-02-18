import os
import json
import matplotlib.pyplot as plt
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
from collections import defaultdict

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

# Compute Average Age by Sex
average_ages = {sex: sum(ages) / len(ages) for sex, ages in age_data.items()}

# Display Results
df_age = pd.DataFrame(list(average_ages.items()), columns=["Sex", "Average Age"])
df_sex_counts = pd.DataFrame(list(sex_counts.items()), columns=["Sex", "Count"])
df_jobs = pd.DataFrame(
    [(sex, job, count) for sex, jobs in job_counts.items() for job, count in jobs.items()],
    columns=["Sex", "Job Title", "Count"]
)

# Plot Age by Sex
plt.figure(figsize=(10, 5))
plt.bar(df_age["Sex"], df_age["Average Age"], color=["blue", "pink"])
plt.xlabel("Sex")
plt.ylabel("Average Age")
plt.title("Average Age by Sex")
plt.show()

# Plot Count by Sex
plt.figure(figsize=(10, 5))
plt.bar(df_sex_counts["Sex"], df_sex_counts["Count"], color=["blue", "pink"])
plt.xlabel("Sex")
plt.ylabel("Count")
plt.title("Number of Individuals by Sex")
plt.show()

# Plot Top 5 Occupations by Sex
top_jobs = df_jobs.groupby("Sex").apply(lambda x: x.nlargest(5, "Count"))
top_jobs.pivot(index="Job Title", columns="Sex", values="Count").plot(kind="bar", figsize=(12, 6))
plt.title("Top 5 Jobs by Sex")
plt.xlabel("Job Title")
plt.ylabel("Count")
plt.legend(title="Sex")
plt.xticks(rotation=45)
plt.show()

consumer.close()
