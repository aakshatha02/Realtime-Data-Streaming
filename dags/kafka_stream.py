import logging
import requests
import uuid
import json
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster

# Airflow DAG default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 26),
    'catchup': False
}

# Function to fetch random user data
def get_data():
    response = requests.get('https://randomuser.me/api/')
    data = response.json()
    return data['results'][0]

# Function to format the data to be sent to Kafka
def format_data(res):
    location = res['location']
    data = {
        'id': str(uuid.uuid4()),  # Generate a unique ID
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, " 
                   f"{location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email']  
    }
    return data

# Function to continuously send messages to Kafka
def send_user_data():
    logging.info("Starting to send user data to Kafka...")

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    end_time = time.time() + 60  # Set end time for 1 minute from now

    while time.time() < end_time:
        try:
            res = get_data()
            data = format_data(res)
            producer.send('user_data_generated', json.dumps(data).encode('utf-8'))
            logging.info(f"Sent data to Kafka: {data}")
            time.sleep(1)  # Sleep for 1 second to prevent flooding
        except Exception as e:
            logging.error(f"An error occurred while sending data to Kafka: {e}")

    producer.close()
    logging.info("Finished sending user data to Kafka.")

# Function to consume first names from Kafka and store in Cassandra
def consume_and_store(**kwargs):
    logging.info("Starting Kafka Consumer...")

    # Setup Kafka consumer
    consumer = KafkaConsumer(
        'user_data_generated',
        bootstrap_servers=['broker:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Manually commit offsets
        group_id='my-group'
    )

    logging.info("Connecting to Cassandra cluster...")
    cluster = Cluster(['cassandra'])  
    session = cluster.connect()

    # Create keyspace and table if they don't exist
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS new_keyspace
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.set_keyspace('new_keyspace')

    session.execute("""
    CREATE TABLE IF NOT EXISTS user_data (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        email TEXT
    )
    """)

    # Poll for messages for a specified time
    end_time = time.time() + 60  # Run for 1 minute

    while time.time() < end_time:
        max_messages = 10  
        polled_messages = consumer.poll(timeout_ms=5000, max_records=max_messages)  # Poll with a 5-second timeout

        if polled_messages:
            for topic_partition, messages in polled_messages.items():
                for message in messages:
                    data = json.loads(message.value.decode('utf-8'))  # Decode the Kafka message
                    logging.info(f"Received data from Kafka: {data}")

                    # Insert the data into Cassandra
                    session.execute("""
                    INSERT INTO user_data (id, first_name,last_name,gender,address,email)
                    VALUES (uuid(), %s, %s,%s,%s,%s)
                    """, (data['first_name'], data['last_name'], data['gender'],data['address'], data['email'],))

                    logging.info(f"Stored in Cassandra: {data['first_name']}")

            # Manually commit the Kafka offset to avoid reprocessing
            consumer.commit()

        else:
            logging.info("No messages consumed within the polling period.")

    logging.info("Time limit reached. Stopping consumer...")

    # Close Cassandra connection
    session.shutdown()
    cluster.shutdown()

    # Close the Kafka consumer
    consumer.close()

# Airflow DAG definition
with DAG('real_time_stream', default_args=default_args, schedule_interval=None) as dag:

    send_message_task = PythonOperator(
        task_id='send_message_to_kafka',
        python_callable=send_user_data
    )

    consume_message_task = PythonOperator(
        task_id='consume_and_store_message',
        python_callable=consume_and_store
    )

    send_message_task >> consume_message_task


