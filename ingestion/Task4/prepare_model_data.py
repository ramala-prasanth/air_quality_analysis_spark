from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler

# Initialize Spark
spark = SparkSession.builder \
    .appName("AirQualityModelPreparation") \
    .getOrCreate()

# Load the cleaned dataset
df = spark.read.option("header", True).option("inferSchema", True).csv("task4_input.csv")

# --- Step 1: Define AQI classification UDF ---
def classify_aqi(pm25):
    if pm25 is None:
        return "Unknown"
    elif pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    else:
        return "Unhealthy"

aqi_udf = udf(classify_aqi, StringType())
df = df.withColumn("AQI_Category", aqi_udf(df["pm25"]))

# --- Step 2: Select features ---
selected_features = [
    "pm25_lag_1",
    "temperature",
    "humidity",
    "pm25_rate_of_change",
    "temperature_rate_of_change",
    "pm25_roll_avg_3",
    "hour"
]

df = df.dropna(subset=selected_features + ["pm25"])

# --- Step 3: Assemble feature vector ---
assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
df_model = assembler.transform(df).select("features", "pm25", "AQI_Category")

# --- Step 4: Train-test split ---
train_df, test_df = df_model.randomSplit([0.8, 0.2], seed=42)

# --- Step 5: Save each as a single CSV file ---
train_df.selectExpr("CAST(features AS STRING)", "pm25", "AQI_Category") \
    .coalesce(1) \
    .write.mode("overwrite").option("header", True) \
    .csv("outputs/train_tmp")

test_df.selectExpr("CAST(features AS STRING)", "pm25", "AQI_Category") \
    .coalesce(1) \
    .write.mode("overwrite").option("header", True) \
    .csv("outputs/test_tmp")

# --- Step 6: Rename part file to a final .csv name ---
import shutil
import os
import glob

def rename_part_file(tmp_dir, final_path):
    part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
    shutil.move(part_file, final_path)
    shutil.rmtree(tmp_dir)

rename_part_file("outputs/train_tmp", "outputs/train_data.csv")
rename_part_file("outputs/test_tmp", "outputs/test_data.csv")

print("✅ Single CSV files created: train_data.csv and test_data.csv")
