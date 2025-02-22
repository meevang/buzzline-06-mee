""" db_sqlite_mee.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.
- delete_message(message_id, db_path): Delete a message from the SQLite database by its ID.
- get_all_messages(db_path): Retrieve all messages from the SQLite database.

Example JSON message
{
    "User ID":"bwendt",
    "Status":"green",
    "Log in":"02/18/25 8:49:39 AM",
    "Last Request":"02/18/25 11:42:22 AM",
    "Description":"Invoice Entry - Brief Mode"
}

"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

__all__ = ['init_db', 'insert_message', 'delete_message', 'get_all_messages']

#####################################
# Define Function to Initialize SQLite Database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    """
    logger.info(f"Calling SQLite init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    "User ID" TEXT,
                    Status TEXT,
                    "Log in" TEXT,
                    "Last Request" TEXT,
                    Description TEXT
                )
            """
            )
            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")

#####################################
# Define Function to Insert a Processed Message into the Database
#####################################

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the SQLite database.

    Args:
    - message (dict): Processed message to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO streamed_messages (
                    "User ID", Status, "Log in", "Last Request", Description
                ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                message["User ID"],
                message["Status"],
                message["Log in"],
                message["Last Request"],
                message["Description"],
            ),
            )
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")

#####################################
# Define Function to Delete a Message from the Database
#####################################

def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from the SQLite database by its ID.

    Args:
    - message_id (int): ID of the message to delete.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")

#####################################
# Define Function to Retrieve All Messages from the Database
#####################################

def get_all_messages(db_path: pathlib.Path) -> list:
    """
    Retrieve all messages from the SQLite database.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    Returns:
    - list: A list of dictionaries, each containing a message's data.
    """
    logger.info(f"Retrieving all messages from database at {db_path}")
    messages = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row  # This enables column access by name
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM streamed_messages")
            rows = cursor.fetchall()
            for row in rows:
                messages.append(dict(row))
        logger.info(f"Retrieved {len(messages)} messages from the database.")
        return messages
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve messages from the database: {e}")
        return []

#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting db testing.")

    # Use config to make a path to a parallel test database
    DATA_PATH: pathlib.path = config.get_base_data_path
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize the SQLite database by passing in the path
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    test_message = {
        "User ID": "cclemens",
        "Status": "green",
        "Log in": "02/18/25 8:45:14 AM",
        "Last Request": "02/18/25 11:22:37 AM",
        "Description": "Job Summary Inquiry",
    }

    insert_message(test_message, TEST_DB_PATH)

    # Retrieve all messages
    all_messages = get_all_messages(TEST_DB_PATH)
    logger.info(f"Retrieved {len(all_messages)} messages from the database.")

    # Delete the test message if it exists
    if all_messages:
        delete_message(all_messages[0]['id'], TEST_DB_PATH)

    logger.info("Finished testing.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
