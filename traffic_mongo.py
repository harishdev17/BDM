from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer('traffic_data', bootstrap_servers='localhost:9092', value_deserializer=lambda v: json.loads(v))
client = MongoClient('mongodb://localhost:27017/')
db = client['traffic_db']
collection = db['traffic_collection']

for message in consumer:
    collection.insert_one(message.value)
