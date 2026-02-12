"""
Pulse — Health Consumer (Day 3)

Reads heartbeats from Kafka topic 'heartbeats',
writes latest server state to Redis, and flags servers
that have gone silent (missed > STALE_THRESHOLD seconds).
"""

import json
import time
import redis
from kafka import KafkaConsumer

TOPIC = "heartbeats"
CONSUMER_GROUP = "pulse-health-monitor"
STALE_THRESHOLD = 15  # seconds without a heartbeat → unhealthy

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",   # only new messages from here on
    enable_auto_commit=True,
)

print(f"[consumer] Subscribed to '{TOPIC}' as group '{CONSUMER_GROUP}'")
print(f"[consumer] Stale threshold: {STALE_THRESHOLD}s\n")


def update_state(payload: dict):
    server_id = payload["id"]
    key = f"server:{server_id}"

    # Write latest heartbeat into Redis hash
    r.hset(key, mapping={
        "status":     payload.get("status", "unknown"),
        "latency_ms": payload.get("latency_ms", -1),
        "last_seen":  payload.get("ts", int(time.time())),
    })

    # Keep a set of all known servers
    r.sadd("servers:known", server_id)

    print(f"  [redis] {server_id} → status={payload['status']}  "
          f"latency={payload['latency_ms']}ms  ts={payload['ts']}")


def check_stale():
    known = r.smembers("servers:known")
    now = int(time.time())
    for server_id in known:
        last_seen = r.hget(f"server:{server_id}", "last_seen")
        if last_seen and (now - int(last_seen)) > STALE_THRESHOLD:
            print(f"  ⚠️  {server_id} is STALE — last seen {now - int(last_seen)}s ago")


last_stale_check = 0

for msg in consumer:
    payload = msg.value
    print(f"[kafka] offset={msg.offset}  partition={msg.partition}  "
          f"key={msg.key}  payload={payload}")
    update_state(payload)

    # Stale check every 10 seconds
    now = time.time()
    if now - last_stale_check > 10:
        check_stale()
        last_stale_check = now
