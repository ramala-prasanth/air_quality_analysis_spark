# common_setup.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Air Quality Analysis") \
    .getOrCreate()

df = spark.read.csv("task3_input.csv", header=True, inferSchema=True)

# Register as global temporary view so it's accessible across scripts
df.createOrReplaceGlobalTempView("air_quality")
