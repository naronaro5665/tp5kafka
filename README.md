## 13. Screenshots and Experimental Evidence

This section presents the practical implementation and experimental results of TP05, following the order of the assigned tasks.

### 13.1. Task 1 – Kafka Broker Configuration Using Docker

**Figure 1. Docker Compose Configuration**

![Docker Compose Configuration](screenshots/compose.png)

**Figure 2. Running Kafka Docker Container**

![Running Kafka Container](screenshots/composedocker.png)

### 13.2. Task 2 – Python Environment and Kafka Client Installation

**Figure 3. Python Environment and Installed Kafka Client**

![Python Environment](screenshots/python2.2.png)

### 13.3. Task 3 – Kafka Producer Implementation

**Figure 4. Basic Producer Sending Sensor Events**

![Basic Producer](screenshots/producer.png)

**Figure 5. Extended Producer Generating 100 Sensor Events**

![Extended Producer](screenshots/python_producer.png)

### 13.4. Task 4 – Kafka Consumer Implementation

**Figure 6. Basic Consumer Receiving Sensor Events**

![Basic Consumer](screenshots/consumer.png)

**Figure 7. Second Consumer Execution**

![Second Consumer](screenshots/consumer2.png)

**Figure 8. Extended Consumer with Running Average, Temperature Alerts, and CSV Storage**

![Extended Consumer](screenshots/consumer_extended.png)

### 13.5. Task 5 – Kafka Topics and Consumer Groups Inspection

**Figure 9. Kafka Topics Inspection**

![Kafka Topics Inspection](screenshots/Inspect%20Kafka%20Topics.png)

**Figure 10. Topic Configuration and Partition Information**

![Topic Configuration](screenshots/Topic.png)

**Figure 11. Consumer Group Information**

![Consumer Group Information](screenshots/Consumer%20group.png)

### 13.6. Further Investigation – Consumer Groups Experiment

**Figure 12. Two Consumers Using the Same Consumer Group**

![Same Consumer Group](screenshots/Same%20Consumer%20Group.png)

**Figure 13. Partition Assignment Between Two Consumers in the Same Group**

![Partition Assignment](screenshots/Message%20Distribution%20Between%20Two%20Consumers%20in%20the%20Same%20Consumer%20Group.png)

**Figure 14. Additional Comparison of Message Distribution**

![Message Distribution Comparison](screenshots/Message%20Distribution%20Between%20Two%20Consumers%20in%20the%20Same%20Consumer%20Group2.png)

### 13.7. Further Investigation – Partition Comparison

**Figure 15. Topic Configuration with Three Partitions**

![Three Partition Configuration](screenshots/comparsion_3partition.png)

**Figure 16. Comparison Between One and Three Partitions**

![One and Three Partition Comparison](screenshots/comparsion_3partitionet1partition.png)

**Figure 17. Single-Partition Topic Configuration**

![Single Partition Topic](screenshots/campus-sensors-single.png)

### 13.8. Further Investigation – Campus Building Information and Filtering

**Figure 18. Filtering Sensor Events for the Labs Building**

![Campus Building Filtering](screenshots/building-filter.png)

### 13.9. Additional Experimental Evidence

**Figure 19. Additional Kafka Terminal Output**

![Additional Experimental Evidence](screenshots/Screenshot%202026-11-05%20171212.png)

---

**End of TP05 – Apache Kafka: Real-Time Data Streaming**