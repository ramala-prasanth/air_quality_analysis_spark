from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("AirQualityModelOptimization") \
    .getOrCreate()

# Load and preprocess data
def read_csv(filepath):
    return spark.read.option("header", True).csv(filepath)

to_vector = udf(lambda row: Vectors.dense([float(x) for x in row.strip('[]').split(',')]), VectorUDT())

train_df = read_csv("/workspaces/air_quality_analysis_spark/ingestion/Task4/outputs/train_data/part-00000-b72779c6-1f91-423d-a0a9-12f1c3be5aa5-c000.csv")
test_df = read_csv("/workspaces/air_quality_analysis_spark/ingestion/Task4/outputs/test_data/part-00000-6e1288a0-6139-48c0-9bef-fd89ea275eb8-c000.csv")

train_df = train_df.withColumn("features", to_vector("features")).withColumn("pm25", train_df["pm25"].cast(DoubleType()))
test_df = test_df.withColumn("features", to_vector("features")).withColumn("pm25", test_df["pm25"].cast(DoubleType()))

# Define model and parameter grid
rf = RandomForestRegressor(featuresCol="features", labelCol="pm25")

paramGrid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.numTrees, [20, 50, 100]) \
    .build()

evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")

# Setup CrossValidator
cv = CrossValidator(estimator=rf,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    numFolds=3,
                    parallelism=2)

# Train and tune
print("🔍 Running cross-validation...")
cv_model = cv.fit(train_df)
best_model = cv_model.bestModel

# Evaluate best model
predictions = best_model.transform(test_df)
rmse = evaluator.evaluate(predictions)
r2 = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="r2").evaluate(predictions)

# Save performance to file
output_path = "outputs/optimized_model_evaluation.txt"
with open(output_path, "w") as f:
    f.write("Hyperparameter Tuning Results (Random Forest Regressor)\n")
    f.write(f"Best maxDepth: {best_model.getOrDefault('maxDepth')}\n")
    f.write(f"Best numTrees: {best_model.getOrDefault('numTrees')}\n")
    f.write(f"RMSE: {rmse:.2f}\n")
    f.write(f"R²: {r2:.2f}\n")

print(f"✅ Optimization complete. Results saved to {output_path}")
