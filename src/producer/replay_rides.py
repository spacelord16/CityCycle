import csv
import json
import time
import logging
from datetime import datetime
from confluent_kafka import Producer
import pandas as pd
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'citycycle-producer'
}
TOPIC = 'citycycle-rides'
DATA_FILE = '../../data/rides.csv'

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        # logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        pass

def load_and_produce():
    if not os.path.exists(DATA_FILE):
        logger.error(f"Data file not found at {DATA_FILE}")
        return

    logger.info("Initializing Kafka Producer...")
    try:
        producer = Producer(KAFKA_CONF)
    except Exception as e:
        logger.error(f"Failed to create Producer: {e}")
        return

    logger.info(f"Reading data from {DATA_FILE}...")
    
    # Read CSV using pandas for easier timestamp handling, but process row by row
    # Using chunksize to avoid loading massive file into memory
    chunk_size = 1000
    
    try:
        for chunk in pd.read_csv(DATA_FILE, chunksize=chunk_size):
            # Ensure timestamps are datetime objects
            if 'started_at' in chunk.columns:
                chunk['started_at'] = pd.to_datetime(chunk['started_at'])
            
            # Sort by start time just in case
            chunk = chunk.sort_values('started_at')
            
            last_timestamp = None

            for _, row in chunk.iterrows():
                # Prepare payload
                payload = {
                    'ride_id': row.get('ride_id'),
                    'rideable_type': row.get('rideable_type'),
                    'started_at': row['started_at'].isoformat(),
                    'ended_at': row['ended_at'],
                    'start_station_name': row.get('start_station_name'),
                    'start_station_id': row.get('start_station_id'),
                    'end_station_name': row.get('end_station_name'),
                    'end_station_id': row.get('end_station_id'),
                    'start_lat': row.get('start_lat'),
                    'start_lng': row.get('start_lng'),
                    'end_lat': row.get('end_lat'),
                    'end_lng': row.get('end_lng'),
                    'member_casual': row.get('member_casual')
                }

                # Simulate real-time delay (optional, speed up factor = 1000x)
                # current_timestamp = row['started_at']
                # if last_timestamp:
                #     time_diff = (current_timestamp - last_timestamp).total_seconds()
                #     if time_diff > 0:
                #         time.sleep(time_diff / 1000) 
                # last_timestamp = current_timestamp

                # Produce to Kafka
                try:
                    producer.produce(
                        TOPIC,
                        key=str(row.get('start_station_id', '')),
                        value=json.dumps(payload),
                        callback=delivery_report
                    )
                    # Trigger any available delivery report callbacks from previous produce() calls
                    producer.poll(0)
                except BufferError:
                    logger.warning("Buffer full, waiting...")
                    producer.flush() 
                except Exception as e:
                    logger.error(f"Error producing record: {e}")

            logger.info(f"Processed chunk of {len(chunk)} records.")
            producer.flush() # Flush after each chunk

    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        producer.flush()
        logger.info("Producer finished.")

if __name__ == "__main__":
    load_and_produce()
