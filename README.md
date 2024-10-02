# Realtime-Data Streaming with Airflow, Kafka, Cassandra and Docker

This project implements a real-time data streaming using Apache Airflow, Apache Kafka, Cassandra and Docker. It fetches random users data from external API(https://randomuser.me/api/) and it streams the data through Kafka and stores it in Cassandra database, all while orchestrating the flow with Airflow. This setup uses Docker containers to orchestrate and manage the services for the easy deployment and scalability.

## Architecture Overview!

<div align="center">
  <img width="694" alt="Architecture Overview" src="https://github.com/aakshatha02/Realtime-Data-Pipeline/blob/main/Architecture.png">
</div>

## Components Involved

- **API (Data Source)**: System fetches the data from external API using Airflow as a scheduler and orchestrator. The data in this case comes from https://randomuser.me/api/ which generates random user data.
  
- **Apache Airflow** : This is used for scheduling the task and orchestrating the workflow. It triggers the data extraction from API and it coordinates the data flow between Kafka and Cassandra.
  
- **PostgreSQL**: A relational Database used by Apache Airflow to store metadata such as dag details, task history.
  
- **Apache Kafka** : A distibuted streaming platform used for building real-time data pipelines. It collects and streams the user data fetched by Airflow. Here, Kafka connects with Zookeeper for service coordination and is responsible for streaming data to the Cassandra database.
  
- **Zookeeper**: It provides distributed coordination for Kafka, handling metadata and maintaining cluster health. It manages broker configurations and state in a Kafka cluster.
  
- **Control Center & Schema Registry**: These are the Confluent platform tools used for monitoring and managing the Kafka topics and brokers (Control Center). Managing and enforcing schemas in Kafka topics, ensuring that data consumed follows predefined structure (Schema Registry).
  
- **Cassandra**: It is distributed NoSQL database that stores the user data streamed from Kafka. Cassandra is used for its horizontal scalability and ability to handle large amounts of real-time data.
  
- **Docker**: The entire setup runs within a Dockerized environment, enabling easy setup, replication, and scaling of the system. All components, including Airflow, Kafka, Zookeeper, Cassandra, and Control Center, are managed as Docker containers.



