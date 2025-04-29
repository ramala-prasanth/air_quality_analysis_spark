from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import desc, count, rank, col, to_timestamp, min, max

# Initialize Spark Session
spark: SparkSession = SparkSession.builder.appName("AirQualityAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load DataFrames
df = spark.read.option("header", True).csv("/Users/samhithdara/PycharmProjects/air_quality_analysis_spark/ingestion/output/preprocessed_csv/part-00000-810010f2-56cb-46ed-b186-966a6cd08d1c-c000.csv")

def explore(df: DataFrame) -> DataFrame:
    # Convert to timestamp type
    # df = df.withColumn("timestamp", to_timestamp("datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    df = df.withColumn("value", col("value").cast("double"))
    df = df.where(col("value") > -999)

    df.show()
    df.select(
        count("value").alias("value_count"),
        min("value").alias("value_min"),
        max("value").alias("value_max"),
    ).show()
    

    return df.select("location", "timestamp", "parameter", "units", "value")

# Save result
explore(df).coalesce(1).write.mode("overwrite").csv("outputf", header=True)