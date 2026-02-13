import json
import logging
from confluent_kafka import Consumer, KafkaError
from neo4j import GraphDatabase
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'citycycle-graph-loader',
    'auto.offset.reset': 'earliest'
}
TOPIC = 'citycycle-rides'

# Neo4j Configuration
NEO4J_URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "citycycle"


class GraphLoader:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password):
        """Initialize Neo4j connection"""
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._create_constraints()
    
    def close(self):
        """Close Neo4j connection"""
        self.driver.close()
    
    def _create_constraints(self):
        """Create uniqueness constraints for efficient MERGE operations"""
        with self.driver.session() as session:
            # Constraint on Station ID
            session.run(
                "CREATE CONSTRAINT station_id IF NOT EXISTS FOR (s:Station) REQUIRE s.id IS UNIQUE"
            )
            logger.info("Constraints created/verified")
    
    def load_ride(self, ride_data):
        """
        Load a single ride into Neo4j graph.
        Creates Station nodes and RIDE relationship.
        """
        with self.driver.session() as session:
            query = """
            // Merge start and end stations
            MERGE (start:Station {id: $start_station_id})
            ON CREATE SET 
                start.name = $start_station_name,
                start.lat = $start_lat,
                start.lng = $start_lng
            
            MERGE (end:Station {id: $end_station_id})
            ON CREATE SET 
                end.name = $end_station_name,
                end.lat = $end_lat,
                end.lng = $end_lng
            
            // Create RIDE relationship
            CREATE (start)-[r:RIDE]->(end)
            SET 
                r.ride_id = $ride_id,
                r.rideable_type = $rideable_type,
                r.started_at = datetime($started_at),
                r.ended_at = datetime($ended_at),
                r.member_casual = $member_casual
            
            RETURN start.id AS start_id, end.id AS end_id
            """
            
            try:
                session.run(query, **ride_data)
            except Exception as e:
                logger.error(f"Error loading ride {ride_data.get('ride_id')}: {e}")


def consume_and_load():
    """Main consumer loop: read from Kafka and load into Neo4j"""
    
    # Wait for Neo4j to be ready
    logger.info("Waiting for Neo4j to be ready...")
    for attempt in range(10):
        try:
            loader = GraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
            logger.info("Connected to Neo4j!")
            break
        except Exception as e:
            if attempt < 9:
                logger.warning(f"Neo4j not ready (attempt {attempt+1}/10), retrying...")
                time.sleep(5)
            else:
                logger.error("Could not connect to Neo4j after 10 attempts")
                return
    
    # Create Kafka consumer
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])
    logger.info(f"Subscribed to topic: {TOPIC}")
    
    try:
        message_count = 0
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Parse message
            try:
                ride_data = json.loads(msg.value().decode('utf-8'))
                loader.load_ride(ride_data)
                message_count += 1
                
                if message_count % 100 == 0:
                    logger.info(f"Processed {message_count} rides")
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        loader.close()
        logger.info(f"Consumer closed. Total rides processed: {message_count}")


if __name__ == "__main__":
    consume_and_load()
