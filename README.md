# TP05 – Apache Kafka: Real-Time Data Streaming

## 1. Project Description

This project demonstrates real-time data streaming using Apache Kafka and Python. It simulates a campus sensor monitoring system that produces temperature and humidity events and sends them to Kafka topics.

The project uses Docker to run a Kafka broker and Python consumers to process incoming messages.

## 2. Objectives

- Configure and run Apache Kafka using Docker Compose.
- Create Kafka topics and partitions.
- Implement Python producers to generate sensor events.
- Implement consumers to receive and process messages.
- Detect high temperatures (temperature >= 35°C).
- Calculate the running average temperature.
- Compare consumers using the same and different consumer groups.
- Compare topics with one partition and three partitions.
- Add campus building information to sensor events.
- Filter events according to the campus building.
- Save sensor events into CSV files.

## 3. Technologies Used

- Apache Kafka 3.9.1
- Docker and Docker Compose
- Python
- kafka-python-ng 2.2.3
- JSON
- CSV

## 4. Project Structure

```text
tp05-kafka/
│
├── compose.yaml
├── requirements.txt
├── producer.py
├── producer_extended.py
├── consumer.py
├── consumer2.py
├── consumer_extended.py
├── consumer_building.py
├── events.csv
├── events_extended.csv
└── screenshots/
```

## 5. Kafka Topics

The following topics were used:

| Topic | Number of Partitions |
|---|---:|
| campus-sensors | 3 |
| campus-sensors-single | 1 |
| campus_building | 3 |

The replication factor is 1 for these topics.

## 6. Producer Implementation

The producer generates sensor events containing:

- Event ID
- Sensor ID
- Temperature
- Humidity
- Timestamp

The extended producer generates 100 events and adds the `campus_building` field.

Four buildings are used:

- Library
- Labs
- Admin
- Dorms

## 7. Consumer Implementation

The consumers perform the following operations:

- Receive sensor events from Kafka.
- Display temperature and humidity values.
- Detect high temperatures.
- Display an alert when temperature is greater than or equal to 35°C.
- Calculate the running average temperature.
- Save received events into CSV files.

## 8. Consumer Groups Experiment

Two consumers were configured with the same consumer group ID:

`tp05-campus-monitor`

The topic `campus-sensors` has three partitions.

Kafka distributed the partitions as follows:

| Consumer | Assigned Partitions |
|---|---|
| Consumer 1 | 0, 1 |
| Consumer 2 | 2 |

This experiment demonstrates partition assignment and parallel message processing within a consumer group.

Consumers using different group IDs can independently consume the topic's messages.

## 9. Campus Building Experiment

The extended producer sends 100 events to `campus-sensors`.

Each event contains a `campus_building` field.

The `consumer_building.py` program filters events and displays only those belonging to the Labs building.

The test successfully displayed event 93, which belonged to Labs.

## 10. Running the Project

Start the Kafka service:

```bash
docker compose up -d
```

Create the topics if they do not already exist.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run the producer:

```bash
python producer.py
```

Run the extended producer:

```bash
python producer_extended.py
```

Run the consumers:

```bash
python consumer.py
```

```bash
python consumer_extended.py
```

Run the building filter:

```bash
python consumer_building.py
```

## 11. Results

The experiments demonstrated:

- Successful Kafka broker configuration.
- Successful creation of topics with different partition counts.
- Successful production and consumption of sensor events.
- High-temperature alert detection.
- Running average temperature calculation.
- Message distribution between consumers in the same group.
- Independent consumption using different consumer groups.
- Successful transmission of the campus building field.
- Successful filtering of events by building.

## 12. Conclusion

This project demonstrates the main concepts of Apache Kafka through a practical campus sensor monitoring application. It shows how producers publish events, how consumers process them, and how partitions and consumer groups support distributed message processing.

The extended experiments also demonstrate how additional event attributes can be used for filtering and monitoring purposes.
## 13. Screenshots

### 1. Kafka and Docker Configuration

![Docker Compose](<screenshots/compose.png>)

![Docker Container](<screenshots/composedocker.png>)

### 2. Kafka Topics

![Kafka Topics](<screenshots/Inspect Kafka Topics.png>)

![Topic Configuration](<screenshots/Topic.png>)

![Single Partition Topic](<screenshots/campus-sensors-single.png>)

### 3. Python Producer

![Python Environment](<screenshots/python2.2.png>)

![Producer](<screenshots/producer.png>)

![Python Producer](<screenshots/python_producer.png>)

### 4. Kafka Consumers

![Consumer](<screenshots/consumer.png>)

![Second Consumer](<screenshots/consumer2.png>)

![Extended Consumer](<screenshots/consumer_extended.png>)

### 5. Consumer Groups and Partition Distribution

![Consumer Group](<screenshots/Consumer group.png>)

![Same Consumer Group](<screenshots/Same Consumer Group.png>)

![Message Distribution](<screenshots/Message Distribution Between Two Consumers in the Same Consumer Group.png>)

![Message Distribution Comparison](<screenshots/Message Distribution Between Two Consumers in the Same Consumer Group2.png>)

### 6. Partition Comparison

![Three Partition Comparison](<screenshots/comparsion_3partition.png>)

![One and Three Partition Comparison](<screenshots/comparsion_3partitionet1partition.png>)

### 7. Campus Building Filtering

![Building Filtering](<screenshots/building-filter.png>)

### 8. Additional Screenshot

![Additional Screenshot](<screenshots/Screenshot 2026-11-05 171212.png>)
