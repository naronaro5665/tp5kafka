"""Further investigation: 100 events + campus_building field."""
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "campus-sensors"
BUILDINGS = ["Library", "Labs", "Admin", "Dorms"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

try:
    for event_id in range(1, 101):
        event = {
            "event_id": event_id,
            "sensor_id": f"sensor-{random.randint(1, 4):02d}",
            "campus_building": random.choice(BUILDINGS),
            "temperature_c": round(random.uniform(18, 42), 2),
            "humidity_pct": round(random.uniform(25, 85), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC, value=event)
        print("Sent:", event)
        time.sleep(0.5)
    producer.flush()
finally:
    producer.close()
