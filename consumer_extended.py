import json
from kafka import KafkaConsumer

TOPIC = "campus-sensors"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="tp05-campus-average",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

total_temperature = 0
event_count = 0

print(f"Listening to topic: {TOPIC}")

try:
    for message in consumer:

        event = message.value

        temperature = event["temperature_c"]

        
        total_temperature += temperature
        event_count += 1

        
        average = total_temperature / event_count

        print(
            f'Event: {event["event_id"]} | '
            f'Sensor: {event["sensor_id"]} | '
            f'Temperature: {temperature}°C | '
            f'Humidity: {event["humidity_pct"]}%'
        )

        print(
            f"Running Average Temperature: {average:.2f}°C"
        )

        if temperature >= 35:
            print("ALERT: High temperature detected!")

        print("-" * 50)

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    consumer.close()
