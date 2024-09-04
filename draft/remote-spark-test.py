from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# this code were tested from the master node
# how to run: docker exec spark-master spark-submit /draft/mounted-spark-test.py
master_uri = "spark://localhost:7077"
spark = SparkSession.builder.appName("Testing PySpark Example")\
    .master(master_uri) \
    .getOrCreate()
    
sample_data = [{"name": "John    D.", "age": 30},
  {"name": "Alice   G.", "age": 25},
  {"name": "Bob  T.", "age": 35},
  {"name": "Eve   A.", "age": 28}]

df = spark.createDataFrame(sample_data)
df.show()