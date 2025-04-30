from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import os
import shutil

# Start Spark session
spark = SparkSession.builder \
    .appName("PM2.5 Pipeline Model") \
    .getOrCreate()

# Load your dataset
data_path = "task4_input.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Define features and clean data
feature_cols = ["pm25_lag_1", "temperature_lag_1", "pm25_rate_of_change", "temperature_rate_of_change"]
df_clean = df.dropna(subset=feature_cols + ["pm25"])

# Set up VectorAssembler and model
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
regressor = RandomForestRegressor(featuresCol="features", labelCol="pm25")

# Build pipeline
pipeline = Pipeline(stages=[assembler, regressor])

# Train/test split
train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)

# Fit pipeline
pipeline_model = pipeline.fit(train_df)

# Evaluate model
predictions = pipeline_model.transform(test_df)
evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2 = evaluator.setMetricName("r2").evaluate(predictions)

print(f"✅ Pipeline model trained. RMSE: {rmse:.2f}, R²: {r2:.2f}")

# Save pipeline model
model_path = "ingestion/Task4/models/pm25_rf_pipeline"
if os.path.exists(model_path):
    shutil.rmtree(model_path)
pipeline_model.save(model_path)

print(f"✅ Pipeline model saved to: {model_path}")
