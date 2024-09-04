# Install necessary packages
# pip install openmeteo-requests requests-cache retry-requests confluent-kafka jsonschema pandas

import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from confluent_kafka import Producer
import json
from jsonschema import validate, ValidationError

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Kafka producer configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)
weather_topic = 'WEATHER'

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

# Function to handle Kafka delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to validate and publish data to Kafka
def publish_to_kafka(data, topic, producer, schema):
    try:
        # Validate data against the schema
        validate(instance=data, schema=schema)
        
        # Produce the message to Kafka
        producer.produce(topic, value=json.dumps(data), callback=delivery_report)
        producer.flush()
    except ValidationError as ve:
        print(f"Data validation error: {ve.message}")
    except Exception as e:
        print(f"Error producing message to Kafka: {e}")

# Fetch data from Open-Meteo API
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "start_date": "2020-12-25",
    "end_date": "2024-01-01",
    "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "snowfall", "weather_code", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_high", "wind_direction_10m", "wind_direction_100m", "soil_temperature_28_to_100cm"],
    "daily": ["temperature_2m_max", "temperature_2m_min"],
    "timezone": "America/New_York"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]

# Process hourly data
hourly = response.Hourly()
hourly_data = {
    "date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ).strftime('%Y-%m-%d %H:%M:%S').tolist(),
    "temperature_2m": hourly.Variables(0).ValuesAsNumpy().tolist(),
    "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy().tolist(),
    "rain": hourly.Variables(2).ValuesAsNumpy().tolist(),
    "snowfall": hourly.Variables(3).ValuesAsNumpy().tolist(),
    "weather_code": hourly.Variables(4).ValuesAsNumpy().tolist(),
    "surface_pressure": hourly.Variables(5).ValuesAsNumpy().tolist(),
    "cloud_cover": hourly.Variables(6).ValuesAsNumpy().tolist(),
    "cloud_cover_low": hourly.Variables(7).ValuesAsNumpy().tolist(),
    "cloud_cover_high": hourly.Variables(8).ValuesAsNumpy().tolist(),
    "wind_direction_10m": hourly.Variables(9).ValuesAsNumpy().tolist(),
    "wind_direction_100m": hourly.Variables(10).ValuesAsNumpy().tolist(),
    "soil_temperature_28_to_100cm": hourly.Variables(11).ValuesAsNumpy().tolist()
}

# Convert hourly data to a DataFrame
hourly_dataframe = pd.DataFrame(data=hourly_data)
print(hourly_dataframe)

# Publish hourly data to Kafka
for index, row in hourly_dataframe.iterrows():
    publish_to_kafka(row.to_dict(), weather_topic, producer, weather_schema)

# Process daily data (if necessary)
# daily = response.Daily()
# daily_data = {
#     "date": pd.date_range(
#         start=pd.to_datetime(daily.Time(), unit="s", utc=True),
#         end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
#         freq=pd.Timedelta(seconds=daily.Interval()),
#         inclusive="left"
#     ).strftime('%Y-%m-%d').tolist(),
#     "temperature_2m_max": daily.Variables(0).ValuesAsNumpy().tolist(),
#     "temperature_2m_min": daily.Variables(1).ValuesAsNumpy().tolist()
# }
# daily_dataframe = pd.DataFrame(data=daily_data)
# print(daily_dataframe)

# Publish daily data to Kafka
# for index, row in daily_dataframe.iterrows():
#     publish_to_kafka(row.to_dict(), weather_topic, producer, weather_schema)

