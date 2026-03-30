from kafka import KafkaProducer
import pandas as pd
import json
import time

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read CSV
df = pd.read_csv("traffic_dataset.csv", parse_dates=['timestamp'])

# Convert Timestamp to string
df['timestamp'] = df['timestamp'].astype(str)

# Send row by row
for _, row in df.iterrows():
    producer.send('traffic_data', value=row.to_dict())
    time.sleep(1)  # simulate real-time
