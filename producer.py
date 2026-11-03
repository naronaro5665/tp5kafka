import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "campus-sensors"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

print("Starting sensor producer...")

try:
    for event_id in range(100):
        event = {
            "event_id": event_id,
            "sensor_id": f"sensor-{random.randint(1, 4):02d}",
            "temperature": round(random.uniform(18, 42), 2),
            "humidity": round(random.uniform(25, 85), 2),
            "campus_building": random.choice([
                "Library",
                "Laboratory",
                "Classroom"
            ]),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        producer.send(TOPIC, value=event)

        print(f"Sent event: {event}")

        time.sleep(1)

    producer.flush()
    print("Finished sending 100 events.")

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()
