# Install necessary packages
# pip install confluent-kafka pandas jsonschema

from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
from jsonschema import validate, ValidationError

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weather_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['WEATHER'])

# Define the schema for validation
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

# Consume messages from Kafka
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Decode the message
        data = json.loads(msg.value().decode('utf-8'))

        # Validate the data against the schema
        try:
            validate(instance=data, schema=weather_schema)
            print(f"Received valid message: {data}")

            # Process the data (example: convert to DataFrame)
            df = pd.DataFrame([data])
            print(df)
        except ValidationError as ve:
            print(f"Invalid message: {ve.message}")

finally:
    consumer.close()
