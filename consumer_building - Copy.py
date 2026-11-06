import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors"
TARGET_BUILDING = "Labs"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="campus-building-filter",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Filtering events for building: {TARGET_BUILDING}")

try:
    for message in consumer:
        event = message.value

        building = event.get("campus_building")

        if building == TARGET_BUILDING:
            print("\nMatching event:")
            print(f"Event ID: {event['event_id']}")
            print(f"Sensor: {event['sensor_id']}")
            print(f"Building: {building}")
            print(f"Temperature: {event['temperature_c']} °C")
            print(f"Humidity: {event['humidity_pct']} %")
            print(f"Timestamp: {event['timestamp']}")
            print("-" * 40)

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
