
import json
import csv
import os
from kafka import KafkaConsumer

CSV_FILE = "events.csv"
FILTER_BUILDING = "Laboratory"

# Create CSV file and write headers if it does not exist
file_exists = os.path.exists(CSV_FILE)

csv_file = open(CSV_FILE, "a", newline="", encoding="utf-8")
csv_writer = csv.writer(csv_file)

if not file_exists or os.path.getsize(CSV_FILE) == 0:
    csv_writer.writerow([
        "event_id",
        "sensor_id",
        "temperature_c",
        "humidity_pct",
        "timestamp",
        "campus_building"
    ])

consumer = KafkaConsumer(
    "campus-sensors",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-monitor",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Waiting for sensor messages...")

total_temperature = 0
count = 0

try:
    for message in consumer:
        event = message.value

        building = event.get("campus_building", "Unknown")

        # Filter only Laboratory events
        if building != FILTER_BUILDING:
            continue

        temperature = event.get(
            "temperature",
            event.get("temperature_c")
        )

        humidity = event.get(
            "humidity",
            event.get("humidity_pct")
        )

        print("\nReceived event:")
        print(f"Building: {building}")
        print(f"Sensor ID: {event.get('sensor_id', 'Unknown')}")
        print(f"Temperature: {temperature} °C")
        print(f"Humidity: {humidity} %")
        print(f"Timestamp: {event.get('timestamp', 'Unknown')}")

        if temperature is not None:
            if temperature >= 35:
                print("!!! WARNING: High temperature detected !!!")

            total_temperature += temperature
            count += 1

            average_temperature = total_temperature / count
            print(f"Running Average Temperature: {average_temperature:.2f} °C")

        # Save the filtered event to CSV
        csv_writer.writerow([
            event.get("event_id", ""),
            event.get("sensor_id", ""),
            temperature,
            humidity,
            event.get("timestamp", ""),
            building
        ])

        csv_file.flush()
        print("Event saved to events.csv")

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()
    csv_file.close()
