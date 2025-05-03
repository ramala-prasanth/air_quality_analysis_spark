import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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

# Function to load data from CSV
def load_data(file_path):
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

# Data Transformation: Feature Engineering (Rolling Averages, Lag, etc.)
def transform_data(df):
    # Example transformation: Dropping rows with missing values in key columns
    feature_cols = ["pm25_lag_1", "temperature_lag_1", "pm25_rate_of_change", "temperature_rate_of_change"]
    df_clean = df.dropna(subset=feature_cols + ["pm25"])
    return df_clean

# Train the Model: Random Forest Regressor
def train_model(df_clean):
    # Feature assembler
    feature_cols = ["pm25_lag_1", "temperature_lag_1", "pm25_rate_of_change", "temperature_rate_of_change"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Random Forest model
    regressor = RandomForestRegressor(featuresCol="features", labelCol="pm25")

    # Build the pipeline
    pipeline = Pipeline(stages=[assembler, regressor])

    # Split data into training and testing sets (80/20)
    train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    pipeline_model = pipeline.fit(train_df)

    # Make predictions
    predictions = pipeline_model.transform(test_df)
    
    return predictions, pipeline_model

# Evaluate Model: Root Mean Squared Error (RMSE) and R²
def evaluate_model(predictions):
    evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    
    evaluator.setMetricName("r2")
    r2 = evaluator.evaluate(predictions)
    
    print(f"✅ Model trained. RMSE: {rmse:.2f}, R²: {r2:.2f}")
    return rmse, r2

# Save predictions to CSV
def save_predictions(predictions, output_path):
    predictions.select("pm25", "prediction", "timestamp").write \
        .format("csv") \
        .option("path", output_path) \
        .option("header", "true") \
        .mode("overwrite") \
        .save()

    print(f"✅ Predictions saved to {output_path}")

# Visualization: Generate plots (Time-series, Spike Events, AQI Classification, and Correlation)
def generate_visualizations(predictions_pd, output_folder):
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Plot Time-series
    plt.figure(figsize=(10, 6))
    plt.plot(predictions_pd['timestamp'], predictions_pd['pm25'], label='Actual PM2.5', color='blue', linestyle='-', marker='o')
    plt.plot(predictions_pd['timestamp'], predictions_pd['prediction'], label='Predicted PM2.5', color='red', linestyle='--', marker='x')
    plt.xlabel('Timestamp')
    plt.ylabel('PM2.5 Levels')
    plt.title('Actual vs Predicted PM2.5 Levels')
    plt.legend()
    plt.savefig(os.path.join(output_folder, 'time_series_plot.png'))
    plt.close()

    # Plot Spike Events
    threshold = 150  # Adjust threshold as needed
    spike_df = predictions_pd[predictions_pd['pm25'] > threshold]
    plt.figure(figsize=(10, 6))
    plt.plot(predictions_pd['timestamp'], predictions_pd['pm25'], label='PM2.5 Levels', color='blue', linestyle='-', marker='o')
    plt.scatter(spike_df['timestamp'], spike_df['pm25'], color='red', label='Spike Event', zorder=5)
    plt.xlabel('Timestamp')
    plt.ylabel('PM2.5 Levels')
    plt.title(f'Spike Events (Threshold > {threshold} µg/m³)')
    plt.legend()
    plt.savefig(os.path.join(output_folder, 'spike_event_plot.png'))
    plt.close()

    # Plot AQI Classification
    aqi_counts = predictions_pd['aqi_classification'].value_counts()
    plt.figure(figsize=(8, 6))
    aqi_counts.plot(kind='bar', color=['green', 'yellow', 'orange', 'red', 'purple'])
    plt.xlabel('AQI Classification')
    plt.ylabel('Frequency')
    plt.title('AQI Classification Breakdown')
    plt.xticks(rotation=45)
    plt.savefig(os.path.join(output_folder, 'aqi_classification_plot.png'))
    plt.close()

    # Correlation Heatmap
    correlation_matrix = predictions_pd[['pm25', 'temperature', 'humidity']].corr()
    plt.figure(figsize=(10, 6))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', cbar=True)
    plt.title('Correlation Heatmap (PM2.5, Temperature, and Humidity)')
    plt.savefig(os.path.join(output_folder, 'correlation_heatmap.png'))
    plt.close()

    print(f"✅ Visualizations saved to {output_folder}")

# Main pipeline function
def main(input_file, output_predictions_path, output_folder):
    # Load data
    df = load_data(input_file)

    # Data transformation (cleaning and feature engineering)
    df_clean = transform_data(df)

    # Train the model and get predictions
    predictions, pipeline_model = train_model(df_clean)

    # Evaluate the model
    rmse, r2 = evaluate_model(predictions)

    # Save predictions
    save_predictions(predictions, output_predictions_path)

    # Convert Spark DataFrame to Pandas DataFrame for visualization
    predictions_pd = predictions.select("timestamp", "pm25", "prediction").toPandas()

    # Classify AQI for visualization
    def classify_aqi(pm25):
        if pm25 <= 50:
            return 'Good'
        elif pm25 <= 100:
            return 'Moderate'
        elif pm25 <= 150:
            return 'Unhealthy for Sensitive Groups'
        elif pm25 <= 200:
            return 'Unhealthy'
        elif pm25 <= 300:
            return 'Very Unhealthy'
        else:
            return 'Hazardous'

    predictions_pd['aqi_classification'] = predictions_pd['pm25'].apply(classify_aqi)

    # Generate visualizations
    generate_visualizations(predictions_pd, output_folder)

if __name__ == "__main__":
    input_file = 'path_to_your_input_file.csv'  # Update with actual input file path
    output_predictions_path = 'output_predictions'  # Update with desired output folder for predictions
    output_folder = 'output/plots'  # Folder where the visualizations will be saved

    main(input_file, output_predictions_path, output_folder)
