from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.ml import PipelineModel

# Step 1: Start Spark Session
spark = SparkSession.builder \
    .appName("RealTimeAirQualityPrediction") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Define schema matching the streaming data
schema = StructType([
    StructField("location", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("pm25", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pm25_lag_1", DoubleType()),
    StructField("temperature_lag_1", DoubleType()),
    StructField("pm25_rate_of_change", DoubleType()),
    StructField("temperature_rate_of_change", DoubleType())
])

# Step 3: Read from TCP stream (e.g., nc -lk 9999)
stream_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 4: Parse CSV line into structured DataFrame
def parse_csv_line(line):
    parts = line.split(",")
    return (
        parts[0],  # location
        parts[1],  # timestamp
        float(parts[2]),
        float(parts[3]),
        float(parts[4]),
        float(parts[5]),
        float(parts[6]),
        float(parts[7]),
        float(parts[8])
    )

parse_udf = udf(parse_csv_line, schema)

structured_df = stream_df.withColumn("parsed", parse_udf("value")).select("parsed.*")

# Step 5: Load trained PipelineModel (which includes feature assembly)
model = PipelineModel.load("/workspaces/air_quality_analysis_spark/ingestion/Task4/models/pm25_rf_pipeline")

# Step 6: Predict using model (features will be auto-generated in the pipeline)
predicted_df = model.transform(structured_df).select("location", "timestamp", "prediction")

# Step 7: Write predictions to CSV files (streaming)
query = predicted_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/realtime_predictions") \
    .option("checkpointLocation", "checkpoints/realtime_prediction") \
    .start()

query.awaitTermination()
