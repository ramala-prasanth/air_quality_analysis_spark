from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AirQualityPrediction") \
    .getOrCreate()

# Define the schema based on your provided data
schema = StructType([
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pm25_scaled", DoubleType(), True),
    StructField("temperature_scaled", DoubleType(), True),
    StructField("humidity_scaled", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("hour", StringType(), True),
    StructField("pm25_roll_avg_3", DoubleType(), True),
    StructField("temperature_roll_avg_3", DoubleType(), True),
    StructField("pm25_lag_1", DoubleType(), True),
    StructField("temperature_lag_1", DoubleType(), True),
    StructField("pm25_rate_of_change", DoubleType(), True),
    StructField("temperature_rate_of_change", DoubleType(), True)
])

# Load data from CSV
data_path = "task4_input.csv"  # Replace with your actual file path
df = spark.read.csv(data_path, header=True, schema=schema)

# Clean data (drop rows with nulls in important columns)
feature_cols = ["pm25_lag_1", "temperature_lag_1", "pm25_rate_of_change", "temperature_rate_of_change"]
df_clean = df.dropna(subset=feature_cols + ["pm25"])

# Check if the dataset is empty
if df_clean.count() == 0:
    print("The dataset is empty. Please check your input file.")
    exit()

# Set up feature assembler and model
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
regressor = RandomForestRegressor(featuresCol="features", labelCol="pm25")

# Build pipeline
pipeline = Pipeline(stages=[assembler, regressor])

# Train/test split (80/20)
train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)

# Train the model
pipeline_model = pipeline.fit(train_df)

# Make predictions
predictions = pipeline_model.transform(test_df)

# Evaluate model performance
evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2 = evaluator.setMetricName("r2").evaluate(predictions)

print(f"✅ Model trained. RMSE: {rmse:.2f}, R²: {r2:.2f}")

# Save predictions to CSV including location, timestamp, temperature, humidity, pm25, and prediction
output_path = "output_predictions"
predictions.select("location", "timestamp", "temperature", "humidity", "pm25", "prediction").write \
    .format("csv") \
    .option("path", output_path) \
    .option("header", "true") \
    .mode("overwrite") \
    .save()

print(f"✅ Predictions saved to {output_path}")
