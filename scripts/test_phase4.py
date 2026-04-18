import requests
import time
from m_kafka import client, streams

BASE_URL = "http://localhost:9092"

def test_metrics():
    print("--- Testing Metrics ---")
    res = requests.get(f"{BASE_URL}/metrics")
    if res.status_code == 200:
        print("Metrics received:")
        print(res.text[:200] + "...")
    else:
        print(f"Error: {res.status_code}")

def test_registry():
    print("\n--- Testing Schema Registry ---")
    res = requests.post(f"{BASE_URL}/subjects", json={
        "subject": "user-event",
        "schema": '{"type": "record", "name": "User", "fields": [{"name": "id", "type": "int"}]}'
    })
    if res.status_code == 200:
        print(f"Schema registered: {res.json()}")
    
    res = requests.get(f"{BASE_URL}/subjects?subject=user-event")
    print(f"Schema fetched: {res.json()['schema']}")

def test_streams():
    print("\n--- Testing Streams (Conceptual) ---")
    k_client = client.KafkaClient(BASE_URL)
    # This would block, so we just show the setup
    s = streams.builder(k_client, "input-topic") \
        .map(lambda x: x.upper()) \
        .filter(lambda x: "SECRET" not in x)
    print("Stream builder configured for 'input-topic' -> sink")

if __name__ == "__main__":
    test_metrics()
    test_registry()
    test_streams()
