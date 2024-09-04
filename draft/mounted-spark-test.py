from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# this code were tested from the host machine as an attemp to connect to the composed spark cluster

master_uri = "spark://spark-master:7077"
spark = SparkSession.builder.appName("Testing PySpark Example")\
    .master(master_uri) \
    .getOrCreate()
    
sample_data = [{"name": "John    D.", "age": 30},
  {"name": "Alice   G.", "age": 25},
  {"name": "Bob  T.", "age": 35},
  {"name": "Eve   A.", "age": 28}]

df = spark.createDataFrame(sample_data)
df.show()