from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from minio import Minio 


minio_url = 'http://minio:9000'
access_key = 'admin'
secret_key = 'password'
bucket_name = "weather-streaming"

client = Minio(
    minio_url.replace('http://', ''),
    access_key=access_key,
    secret_key=secret_key,
    secure=False  # Set to True if you're using HTTPS
)

# # Create a bucket
try:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")
except Exception as e:
    print(f"Error creating bucket: {e}")
    
# Define schema based on the Kafka message structure
schema = StructType([
    StructField("date", StringType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snowfall", DoubleType(), True),
    StructField("weather_code", LongType(), True),
    StructField("surface_pressure", DoubleType(), True),
    StructField("cloud_cover", DoubleType(), True),
    StructField("cloud_cover_low", DoubleType(), True),
    StructField("cloud_cover_high", DoubleType(), True),
    StructField("wind_direction_10m", DoubleType(), True),
    StructField("wind_direction_100m", DoubleType(), True),
    StructField("soil_temperature_28_to_100cm", DoubleType(), True)
])

# Initialize Spark Session
config = SparkConf() \
        .set("spark.jars.packages", "com.github.jnr:jnr-posix:3.1.15,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .set("fs.s3a.access.key", access_key) \
        .set("fs.s3a.secret.key", secret_key) \
        .set("fs.s3a.endpoint", minio_url) \
        .set("fs.s3a.path.style.access", "true") \
        .set("fs.s3a.connection.ssl.enabled", "false") \
        .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")   
  
spark = SparkSession.builder \
    .appName("KafkaWeatherConsumer") \
    .master("spark://spark-master:7077") \
    .config(conf=config) \
    .getOrCreate()

# Read from Kafka
weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "WEATHER") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the Kafka messages as JSON
weather_df_parsed = weather_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Processing logic (here we simply print the data)
query = weather_df_parsed \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", f"s3a://{bucket_name}/stream_output") \
    .option("checkpointLocation", f"s3a://{bucket_name}/checkpoint") \
    .start()

query.awaitTermination()