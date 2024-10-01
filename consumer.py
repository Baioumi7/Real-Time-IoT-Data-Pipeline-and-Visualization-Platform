################### kafka to es ###############

from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from jsonschema import validate, ValidationError
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Change to 'broker:29092' if running in Docker
    'group.id': 'weather-data-consumer',
    'auto.offset.reset': 'earliest'
    #.option("startingOffsets", "latest") \
}

# Elasticsearch configuration
es_host = "http://localhost:9200"  # Change to 'http://es:9200' if running in Docker
index_name = "weather"

# Define the schema for the Kafka messages
weather_schema = {
    "type": "object",
    "properties": {
        "date": {"type": "string", "format": "date-time"},
        "temperature_2m": {"type": "number"},
        "relative_humidity_2m": {"type": "number"},
        "rain": {"type": "number"},
        "snowfall": {"type": "number"},
        "weather_code": {"type": "number"},
        "surface_pressure": {"type": "number"},
        "cloud_cover": {"type": "number"},
        "cloud_cover_low": {"type": "number"},
        "cloud_cover_high": {"type": "number"},
        "wind_direction_10m": {"type": "number"},
        "wind_direction_100m": {"type": "number"},
        "soil_temperature_28_to_100cm": {"type": "number"}
    },
    "required": ["date", "temperature_2m", "relative_humidity_2m"]
}

def create_es_client():
    """Creates an Elasticsearch client."""
    return Elasticsearch([es_host])

def create_kafka_consumer(topic):
    """Creates a Kafka consumer."""
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    return consumer

def ensure_index_exists(es_client, index_name):
    """Ensures the Elasticsearch index exists, creating it if necessary."""
    if not es_client.indices.exists(index=index_name):
        try:
            es_client.indices.create(index=index_name)
            logging.info(f"Index '{index_name}' created successfully")
        except Exception as e:
            logging.error(f"Error creating index '{index_name}': {e}")
            raise

def index_to_elasticsearch(es_client, data, index_name):
    """Indexes a single document to Elasticsearch."""
    try:
        response = es_client.index(index=index_name, document=data)
        if response['result'] == 'created':
            logging.info(f"Successfully indexed data for {data['date']}")
        else:
            logging.warning(f"Unexpected result when indexing data: {response['result']}")
    except Exception as e:
        logging.error(f"Error indexing data: {e}")

def main():
    es_client = create_es_client()
    ensure_index_exists(es_client, index_name)
    
    weather_topic = 'WEATHER'
    consumer = create_kafka_consumer(weather_topic)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                validate(instance=data, schema=weather_schema)
                index_to_elasticsearch(es_client, data, index_name)
            except json.JSONDecodeError as je:
                logging.error(f"JSON decode error: {je}")
            except ValidationError as ve:
                logging.error(f"Data validation error: {ve.message}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    finally:
        consumer.close()
        logging.info("Consumer closed, script ended")

if __name__ == "__main__":
    main()