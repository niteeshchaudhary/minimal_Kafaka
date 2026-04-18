import requests
import struct
import time
import subprocess
import os
import signal
from pprint import pprint

BASE_URL1 = "http://localhost:9092"
BASE_URL2 = "http://localhost:9093"

def produce_message(url, topic, key, value, acks="1"):
    res = requests.post(f"{url}/produce?acks={acks}", json={
        "topic": topic,
        "key": key,
        "value": value,
        "acks": acks
    })
    return res

def consume_binary(url, topic, partition, offset):
    res = requests.get(f"{url}/consume/binary?topic={topic}&partition={partition}&offset={offset}&limit=1")
    return res

def test_acks_all():
    topic = "test_acks"
    print(f"--- Testing acks=all for {topic} ---")
    
    # 1. Produce with acks=all
    # This should return relatively quickly if a replica is already fetching.
    print("Producing with acks=all (expecting wait for replication)...")
    start = time.time()
    res = produce_message(BASE_URL1, topic, "k_acks", "v_acks", acks="all")
    end = time.time()
    
    if res.status_code == 200:
        print(f"Success! Status: 200, Latency: {end-start:.2f}s")
        pprint(res.json())
    else:
        print(f"Failed! Status: {res.status_code}, Error: {res.text}")

def test_replication():
    topic = "test_repl"
    print(f"\n--- Testing Replication for {topic} ---")
    
    # 1. Produce to Broker 1
    produce_message(BASE_URL1, topic, "k_repl", "v_repl", acks="1")
    print("Produced to Broker 1")
    
    # 2. Wait for replication
    print("Waiting for Broker 2 to sync...")
    time.sleep(3)
    
    # 3. Consume from Broker 2
    res = requests.get(f"{BASE_URL2}/consume?topic={topic}&partition=0&offset=0")
    if res.status_code == 200:
        msgs = res.json()
        if len(msgs) > 0:
            print("Broker 2 has the message!")
            pprint(msgs[0])
        else:
            print("Broker 2 is empty!")
    else:
        print(f"Error fetching from Broker 2: {res.text}")

if __name__ == "__main__":
    # Note: Assumes Broker 1 is on 9092 and Broker 2 is on 9093
    # And Broker 2 is configured to follow Broker 1
    test_replication()
    test_acks_all()
