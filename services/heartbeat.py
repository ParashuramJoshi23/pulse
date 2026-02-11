
import sys
import json
SERVER_ID = sys.argv[1] # Pass server-1, server-2, server-3
from kafka import KafkaProducer
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"[{SERVER_ID}] Starting heartbeat.. ")

while True:
    payload = {
        "id": SERVER_ID,
        "status": "healthy",
        "latency_ms": random.randint(10,200),
        "ts": int(time.time())
    }
    producer.send("heartbeats", value=payload)
    print(f"[{SERVER_ID}] Sent: {payload}")
    time.sleep(5)