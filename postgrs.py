import json
from confluent_kafka import Consumer, KafkaError
import psycopg2
from datetime import datetime

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weather-data-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['WEATHER'])

# PostgreSQL Configuration
db_params = {
    'host': 'localhost',
    'database': 'weatherdb',
    'user': 'bio',
    'password': 'bayoumii77',
    'port': '5432'
}

# Function to create database if not exists
def create_database_if_not_exists():
    # Connect to 'postgres' database first
    conn = psycopg2.connect(
        host=db_params['host'],
        database='postgres',
        user=db_params['user'],
        password=db_params['password'],
        port=db_params['port']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Check if our database exists
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_params['database'],))
    exists = cursor.fetchone()
    if not exists:
        print(f"Creating database {db_params['database']}...")
        cursor.execute(f"CREATE DATABASE {db_params['database']}")
    else:
        print(f"Database {db_params['database']} already exists.")
    
    cursor.close()
    conn.close()

# Create weatherdb database if it doesn't exist
create_database_if_not_exists()

# Now connect to our weatherdb
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Create table if not exists
create_table_query = '''
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    temperature_2m FLOAT,
    relative_humidity_2m FLOAT,
    rain FLOAT,
    snowfall FLOAT,
    weather_code INT,
    surface_pressure FLOAT,
    cloud_cover FLOAT,
    cloud_cover_low FLOAT,
    cloud_cover_high FLOAT,
    wind_direction_10m FLOAT,
    wind_direction_100m FLOAT,
    soil_temperature_28_to_100cm FLOAT
)
'''
cursor.execute(create_table_query)
conn.commit()

# Function to insert data into PostgreSQL
def insert_weather_data(data):
    insert_query = '''
    INSERT INTO weather_data (date, temperature_2m, relative_humidity_2m, rain, snowfall, weather_code, 
                             surface_pressure, cloud_cover, cloud_cover_low, cloud_cover_high, 
                             wind_direction_10m, wind_direction_100m, soil_temperature_28_to_100cm)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    cursor.execute(insert_query, (
        datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S'),
        data['temperature_2m'],
        data['relative_humidity_2m'],
        data.get('rain'),
        data.get('snowfall'),
        data.get('weather_code'),
        data.get('surface_pressure'),
        data.get('cloud_cover'),
        data.get('cloud_cover_low'),
        data.get('cloud_cover_high'),
        data.get('wind_direction_10m'),
        data.get('wind_direction_100m'),
        data.get('soil_temperature_28_to_100cm')
    ))
    conn.commit()

# Main consumer loop
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print(f'Error: {msg.error()}')
        else:
            print(f'Received message: {msg.value().decode("utf-8")}')
            try:
                weather_data = json.loads(msg.value().decode('utf-8'))
                insert_weather_data(weather_data)
                print(f"Inserted data for date: {weather_data['date']}")
            except Exception as e:
                print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print('Interrupted by user')

finally:
    consumer.close()
    cursor.close()
    conn.close()