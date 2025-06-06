import requests
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = 'https://api.openf1.org/v1'  # Example API

def fetch_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"API error: {e}")
        return []

def produce(topic):
    data = fetch_data()
    for item in data:
        producer.send(topic, item)
        print(f"Sent to Kafka: {item}")
    producer.flush()

if __name__ == "__main__":
    while True:
        produce("my-api-topic")
        time.sleep(60)  # Poll every 60 seconds
