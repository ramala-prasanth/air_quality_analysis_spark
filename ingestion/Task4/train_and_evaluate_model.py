from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("AirQualityModelTraining") \
    .getOrCreate()

# Helper to read a single-part CSV
def read_csv(filepath):
    return spark.read.option("header", True).csv(filepath)

# Read datasets
train_df = read_csv("/workspaces/air_quality_analysis_spark/ingestion/Task4/outputs/train_data/part-00000-b72779c6-1f91-423d-a0a9-12f1c3be5aa5-c000.csv")
test_df = read_csv("/workspaces/air_quality_analysis_spark/ingestion/Task4/outputs/test_data/part-00000-6e1288a0-6139-48c0-9bef-fd89ea275eb8-c000.csv")

# Convert 'features' from string to Spark vector
to_vector = udf(lambda row: Vectors.dense([float(x) for x in row.strip('[]').split(',')]), VectorUDT())
train_df = train_df.withColumn("features", to_vector("features")).withColumn("pm25", train_df["pm25"].cast(DoubleType()))
test_df = test_df.withColumn("features", to_vector("features")).withColumn("pm25", test_df["pm25"].cast(DoubleType()))

# Prepare output file
output_path = "outputs/model_evaluation.txt"
with open(output_path, "w") as f:

    # --- Regression Model ---
    f.write("📈 Random Forest Regression (PM2.5 Prediction)\n")
    rf_reg = RandomForestRegressor(featuresCol="features", labelCol="pm25")
    reg_model = rf_reg.fit(train_df)
    reg_preds = reg_model.transform(test_df)

    reg_rmse = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse").evaluate(reg_preds)
    reg_r2 = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="r2").evaluate(reg_preds)

    f.write(f"✅ RMSE: {reg_rmse:.2f}\n")
    f.write(f"✅ R²: {reg_r2:.2f}\n\n")

    # --- Classification Model ---
    f.write("📊 Random Forest Classification (AQI Category Prediction)\n")
    train_df_cls = StringIndexer(inputCol="AQI_Category", outputCol="label").fit(train_df).transform(train_df)
    test_df_cls = StringIndexer(inputCol="AQI_Category", outputCol="label").fit(test_df).transform(test_df)

    rf_cls = RandomForestClassifier(featuresCol="features", labelCol="label")
    cls_model = rf_cls.fit(train_df_cls)
    cls_preds = cls_model.transform(test_df_cls)

    cls_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(cls_preds)
    cls_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(cls_preds)

    f.write(f"✅ Accuracy: {cls_acc:.2f}\n")
    f.write(f"✅ F1 Score: {cls_f1:.2f}\n")

print(f"✅ Model evaluation results saved to: {output_path}")
