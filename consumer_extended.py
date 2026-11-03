"""Further investigation: running average, building filter, CSV export.
Usage: python consumer_extended.py [group_id] [building]
"""
import csv
import json
import sys

from kafka import KafkaConsumer

TOPIC = "campus-sensors"
GROUP = sys.argv[1] if len(sys.argv) > 1 else "tp05-extended"
BUILDING = sys.argv[2] if len(sys.argv) > 2 else None
FIELDS = ["event_id", "sensor_id", "temperature_c", "humidity_pct", "timestamp"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

count, total = 0, 0.0
print(f"Group={GROUP} | building filter={BUILDING}")

with open("events.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()
    try:
        for msg in consumer:
            e = msg.value
            if BUILDING and e.get("campus_building") != BUILDING:
                continue
            count += 1
            total += e["temperature_c"]
            writer.writerow(e)
            f.flush()
            flag = "  ALERT!" if e["temperature_c"] >= 35 else ""
            print(
                f'[p{msg.partition} off={msg.offset}] {e["sensor_id"]} '
                f'T={e["temperature_c"]}°C | running avg={total / count:.2f}°C{flag}'
            )
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        consumer.close()
