import json
import urllib.request
import urllib.parse
from pprint import pprint

BASE_URL = "http://localhost:8080"

def produce(topic: str, key: str, value: str):
    url = f"{BASE_URL}/produce"
    data = {"topic": topic, "key": key, "value": value}
    req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'), headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req) as f:
            resp = json.loads(f.read().decode('utf-8'))
            print(f"✅ Produced -> {resp}")
            return resp
    except Exception as e:
        print(f"❌ Failed to produce: {e}")

def consume(topic: str, partition: int, offset: int):
    query = urllib.parse.urlencode({
        "topic": topic,
        "partition": partition,
        "offset": offset
    })
    url = f"{BASE_URL}/consume?{query}"
    req = urllib.request.Request(url, method='GET')
    try:
        with urllib.request.urlopen(req) as f:
            resp = json.loads(f.read().decode('utf-8'))
            if resp:
                print(f"✅ Consumed {len(resp)} messages:")
                for msg in resp:
                    pprint(msg)
            else:
                print("✅ No new messages.")
            return resp
    except Exception as e:
        print(f"❌ Failed to consume: {e}")

if __name__ == "__main__":
    print("=== Minimal Kafka Client ===")
    
    # 1. Produce some messages
    produce(topic="logs", key="app1", value="Server started")
    produce(topic="logs", key="app1", value="User logged in")
    produce(topic="logs", key="app2", value="Background job running")
    
    # 2. Consume from topic 'logs', partition 0
    print("\n--- Fetching Partition 0 ---")
    consume(topic="logs", partition=0, offset=0)
    
    print("\n--- Fetching Partition 1 ---")
    consume(topic="logs", partition=1, offset=0)
    
    print("\n--- Fetching Partition 2 ---")
    consume(topic="logs", partition=2, offset=0)
