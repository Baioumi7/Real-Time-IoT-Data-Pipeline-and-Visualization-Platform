from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
########spark with es############
# Initialize Spark session with required configurations for Kafka and Elasticsearch
spark = SparkSession.builder.appName("Read From Kafka") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Set up Elasticsearch connection
es = Elasticsearch(hosts=["localhost:9200"])

# Define index settings and mapping for Elasticsearch (updated to match producer schema)
weather_index = {
    # "settings": {
    #     "index": {
    #         "analysis": {
    #             "analyzer": {
    #                 "custom_analyzer": {
    #                     "type": "custom",
    #                     "tokenizer": "standard"
    #                 }
    #             }
    #         }
    #     }
    # },
    "mappings": {
        "properties": {
            "date": {"type": "date"},
            "temperature_2m": {"type": "float"},
            "relative_humidity_2m": {"type": "float"},
            "rain": {"type": "float"},
            "snowfall": {"type": "float"},
            "weather_code": {"type": "integer"},
            "surface_pressure": {"type": "float"},
            "cloud_cover": {"type": "float"},
            "cloud_cover_low": {"type": "float"},
            "cloud_cover_high": {"type": "float"},
            "wind_direction_10m": {"type": "float"},
            "wind_direction_100m": {"type": "float"},
            "soil_temperature_28_to_100cm": {"type": "float"}
        }
    }
}

# Create the Elasticsearch index with the mapping
if not es.indices.exists(index="weather-index"):
    es.indices.create(index="weather-index", body=weather_index)

# Read the stream from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "WEATHER") \
    .option("failOnDataLoss", "false") \
    .load()

# Select and process the necessary columns
df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")

# Define schema for the incoming data
schema = StructType([
    StructField("date", StringType(), True),
    StructField("temperature_2m", FloatType(), True),
    StructField("relative_humidity_2m", FloatType(), True),
    StructField("rain", FloatType(), True),
    StructField("snowfall", FloatType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("surface_pressure", FloatType(), True),
    StructField("cloud_cover", FloatType(), True),
    StructField("cloud_cover_low", FloatType(), True),
    StructField("cloud_cover_high", FloatType(), True),
    StructField("wind_direction_10m", FloatType(), True),
    StructField("wind_direction_100m", FloatType(), True),
    StructField("soil_temperature_28_to_100cm", FloatType(), True)
])

# Parse JSON strings into columns based on the schema
df3 = df2.withColumn("jsonData", F.from_json(F.col("value"), schema)) \
    .select("jsonData.*")

# Convert date column from string to timestamp
df3 = df3.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss"))

# Write the data to Elasticsearch
def writeToElasticsearch(df, epoch_id):
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.resource", "weather-index") \
        .mode("append") \
        .save()

# Stream and write to Elasticsearch
streamingQuery = df3.writeStream \
    .trigger(processingTime="1 second") \
    .option("numRows", 4) \
    .option("truncate", False) \
    .outputMode("append") \
    .foreachBatch(writeToElasticsearch) \
    .start()

streamingQuery.awaitTermination()
