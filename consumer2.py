
import json
from kafka import KafkaConsumer

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

        
        temperature = event.get(
            "temperature",
            event.get("temperature_c")
        )

        humidity = event.get(
            "humidity",
            event.get("humidity_pct")
        )

        
        print("\nReceived event:")
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

            print(
                f"Running Average Temperature: "
                f"{average_temperature:.2f} °C"
            )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    
    consumer.close()
    print("Consumer connection closed.")
