"""
producer_mee.py

Stream JSON data to a file and - if available - a Kafka topic.

Example JSON message
{
"User_ID": "bwendt",
"Status": "green",
"Log_in": "2/18/2025 8:49",
"Last_Request": "2/18/2025 11:42",
"Description": "Invoice Entry - Brief Mode"
}

Environment variables are in utils/utils_config module.
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
import time
from datetime import datetime, timedelta
import random

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Define Message Generator
#####################################

def generate_messages():
    """
    Generate a stream of JSON messages from predefined data.
    """
    users = ["ajurek", "bwendt", "cclemens", "dsmith", "ejones"]
    statuses = ["green", "yellow", "red"]
    descriptions = [
        "Invoice Entry - Brief Mode",
        "Service Orders for Scheduling",
        "Job Summary Inquiry",
        "Customer Account Management",
        "Inventory Check"
    ]

    while True:
        current_time = datetime.now()
        login_time = current_time - timedelta(hours=random.randint(1, 8))
        last_request_time = current_time - timedelta(minutes=random.randint(1, 60))

        message = {
            "User_ID": random.choice(users),
            "Status": random.choice(statuses),
            "Log_in": login_time.strftime("%m/%d/%Y %H:%M"),
            "Last_Request": last_request_time.strftime("%m/%d/%Y %H:%M"),
            "Description": random.choice(descriptions)
        }
        yield message

#####################################
# Define Main Function
#####################################

def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
        logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        for message in generate_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 5a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 5b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
