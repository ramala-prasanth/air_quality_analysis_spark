from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr, trim
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("AirQualityStreamingUpdated") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Read raw stream from TCP
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
# 3. Define the schema for the incoming data
schema = StructType() \
    .add("location_id", StringType()) \
    .add("sensors_id", StringType()) \
    .add("location", StringType()) \
    .add("datetime", StringType()) \
    .add("lat", DoubleType()) \
    .add("lon", DoubleType()) \
    .add("parameter", StringType()) \
    .add("units", StringType()) \
    .add("value", DoubleType())

# 4. Parse the incoming stream (assuming the raw stream is CSV-like data)
parsed_stream = raw_stream.select(
    F.split(
        F.regexp_replace(F.regexp_replace(raw_stream['value'], "[\\[\\]']", ""), "\\s+", ""),
        ','
    ).alias('cols')
)

parsed_stream = parsed_stream.select(
    F.col('cols')[0].alias('location_id'),
    F.col('cols')[1].alias('sensors_id'),
    F.col('cols')[2].alias('location'),
    F.col('cols')[3].alias('datetime'),
    F.col('cols')[4].cast('double').alias('lat'),
    F.col('cols')[5].cast('double').alias('lon'),
    F.col('cols')[6].alias('parameter'),
    F.col('cols')[7].alias('units'),
    F.col('cols')[8].cast('double').alias('value')
).filter(~F.col("location_id").contains("location_id"))  # Skip header row if present


# 5. Convert the 'datetime' string to a timestamp
parsed_stream = parsed_stream.withColumn(
    "timestamp", to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ssXXX")
)

# 6. Add watermark to handle late data (10 minutes in this case)
parsed_stream_with_watermark = parsed_stream.withWatermark("timestamp", "10 minutes")

query = parsed_stream_with_watermark.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/air_quality_csv") \
    .option("checkpointLocation", "output/checkpoints") \
    .option("header", "true") \
    .start()

# 9. Await termination
query.awaitTermination()
