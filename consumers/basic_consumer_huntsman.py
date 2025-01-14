# basic_consumer_huntsman.py

"""
Read a log file as it is being written. 
Process each message and perform analytics or alert on special conditions.
"""

#####################################
# Import Modules
#####################################

# Import sys and os to adjust the Python path for module resolution
import sys
import os

# Add the project root directory to sys.path so the 'utils' module can be found
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # Go up one level to the project root
sys.path.append(project_root)

# Now import functions from the 'utils' module
from utils.utils_logger import logger, get_log_file_path

# Import standard library packages
import time

#####################################
# Define a function to process a single message
#####################################

def process_message(log_file: str) -> None:
    """
    Read a log file in real-time and process each new message.

    Args:
        log_file (str): The path to the log file to read.
    """
    try:
        with open(log_file, "r") as file:
            # Move to the end of the file to start reading new entries
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new log messages...")

            # Infinite loop to continuously read the file in real-time
            while True:
                current_position = file.tell()  # Get the current position in the file
                line = file.readline()  # Read the next line

                if not line:
                    file.seek(current_position)  # Revert to the position where we last read
                    print("No new log message, waiting...")  # Debugging: inform consumer is waiting
                    time.sleep(1)  # Wait for a new line to be written
                    continue

                # Process the new log message
                message = line.strip()  # Remove any extra whitespace
                print(f"Consumed log message: {message}")  # Debugging: print the message

                # Perform analytics or alerting on specific patterns in the message
                perform_analytics(message)

    except FileNotFoundError:
        print(f"Error: The log file {log_file} was not found.")
        logger.error(f"FileNotFoundError: The log file {log_file} was not found.")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        logger.error(f"Unexpected error occurred: {e}")

#####################################
# Define a function to perform analytics or alerting
#####################################

def perform_analytics(message: str) -> None:
    """
    Perform real-time analytics on the log message and trigger alerts.

    Args:
        message (str): The log message to analyze.
    """
    # Example patterns to match (these can be customized)
    if "ERROR" in message:
        print(f"ALERT: Error detected! \n{message}")
        logger.error(f"ALERT: Error detected! \n{message}")
    
    elif "WARNING" in message:
        print(f"ALERT: Warning detected! \n{message}")
        logger.warning(f"ALERT: Warning detected! \n{message}")
    
    # Example custom pattern (you can define any custom message here)
    if "loved a movie!" in message:
        print(f"ALERT: The special message was found! \n{message}")
        logger.warning(f"ALERT: The special message was found! \n{message}")

#####################################
# Define main function for this script
#####################################

def main() -> None:
    """Main entry point."""

    logger.info("START consumer...")

    # Get the log file path from a utility function
    log_file_path = get_log_file_path()
    print(f"Reading from log file: {log_file_path}")  # Debugging: check the file path
    logger.info(f"Reading from log file: {log_file_path}")

    try:
        process_message(log_file_path)
    except KeyboardInterrupt:
        print("User stopped the process.")
    
    logger.info("END consumer.....")

#####################################
# Conditional Execution
#####################################

# If this file is the one being executed, call the main() function
if __name__ == "__main__":
    main()
