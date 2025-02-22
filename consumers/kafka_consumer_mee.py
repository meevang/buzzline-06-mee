import json
import os
import pathlib
import sys
import time
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import defaultdict

from kafka import KafkaConsumer

import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_mee import init_db, insert_message



# Initialize data structures
user_status_data = defaultdict(dict)  # To store user data for plotting

# Set up the plot
fig, ax = plt.subplots(figsize=(12, 6))
plt.ion()


def update_chart(frame):
    """Updates the bar chart with the latest user status data."""
    ax.clear()

    users = list(user_status_data.keys())
    statuses = [data['Status'] for data in user_status_data.values()]

    color_map = {'green': 'green', 'yellow': 'yellow', 'red': 'red'}
    colors = [color_map.get(status, 'gray') for status in statuses]

    if not users:
        ax.text(0.5, 0.5, "No data to display", ha='center', va='center')
    else:
        bars = ax.bar(users, [1] * len(users), color=colors)
        ax.set_xlabel("Users")
        ax.set_ylabel("Status")
        ax.set_title("Real-Time User Status")
        ax.set_ylim(0, 1.2)  # Add a small buffer to the top

        for bar, status in zip(bars, statuses):
            ax.text(bar.get_x() + bar.get_width() / 2., 0.5,
                    status,
                    ha='center', va='center', rotation=90)

        plt.xticks(rotation=45, ha="right")

    plt.tight_layout()



def process_message(message: dict) -> dict:
    """Processes a single message, extracting relevant fields."""
    logger.info(f"Processing message: {message}")
    try:
        processed_message = {
            "User_ID": message.get("User_ID"),
            "Status": message.get("Status"),
            "Log_in": message.get("Log_in"),
            "Last_Request": message.get("Last_Request"),
            "Description": message.get("Description"),
            "timestamp": message.get("timestamp")  # Include timestamp
        }

        user_id = processed_message["User_ID"]
        user_status_data[user_id] = processed_message  # Update global dictionary

        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None




def consume_messages_from_kafka(topic, kafka_url, group, sql_path, interval_secs):
    """Consumes messages from a Kafka topic and updates the plot."""
    consumer = None  # Initialize consumer outside try block
    try:
        logger.info("Step 1. Verify Kafka Services.")
        verify_services()

        logger.info("Step 2. Create a Kafka consumer.")
        consumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )

        logger.info("Step 3. Verify topic exists.")
        if not is_topic_available(topic):
            raise ValueError(f"Topic '{topic}' does not exist.")
        logger.info(f"Kafka topic '{topic}' is ready.")

        logger.info("Step 4. Process messages.")
        last_update_time = time.time()

        for message in consumer:
            processed_message = process_message(message.value) # process json messages

            if processed_message:
                insert_message(processed_message, sql_path)  # Insert into SQLite DB
                logger.info(f"Inserted message into database: {processed_message}")



            current_time = time.time()
            if current_time - last_update_time >= 5:
                update_chart(0)  # Update chart data & redraw
                plt.draw()
                plt.pause(0.001)  # Tiny pause to allow the plot to update
                last_update_time = current_time



    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed.")



def main():
    """Main function to set up and run the Kafka consumer and plot."""
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs = config.get_message_interval_seconds_as_int()
        sqlite_path = config.get_sqlite_path()

        if sqlite_path.exists():
            sqlite_path.unlink()  # Delete if exists to start clean

        init_db(sqlite_path)  # Initialize SQLite Database



        ani = FuncAnimation(fig, update_chart, interval=5000)  # Set up the animation
        plt.show(block=False)  # Show the plot without blocking


        consume_messages_from_kafka(topic, kafka_url, group_id, sqlite_path, interval_secs)


    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")
        plt.close()


if __name__ == "__main__":
    main()
