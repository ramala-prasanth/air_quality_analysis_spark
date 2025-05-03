# Air Quality Monitoring & Forecasting Pipeline

## Overview

This project builds an end-to-end pipeline for monitoring and forecasting air quality metrics (PM2.5) and associated factors (temperature and humidity). The goal is to ingest near-real-time air quality data, perform data preprocessing, aggregate and transform the data, apply Spark SQL for trend analysis, train machine learning models using Spark MLlib, and visualize the results on an interactive dashboard. The pipeline is designed to process both historical and real-time data streams.



## Project Modules

### Section 1: Data Ingestion and Initial Pre-Processing

#### Objective:

* Ingest air quality data from a TCP server (simulating real-time data).
* Clean the data by parsing timestamps, removing irrelevant fields, and validating the schema.
* Merge multiple metrics (PM2.5, temperature, humidity) into a unified record.
* Perform initial checks for data quality.

#### Expected Output:

* A raw DataFrame ready for further analysis and processing.

#### Files:

* `Task1/`: Reads raw data from a TCP server and performs event-time watermarking.
* `Task2/`: Parses timestamps, drops irrelevant fields, and validates schema.

### Section 2: Data Aggregations, Transformations & Trend Analysis

#### Objective:

* Handle outliers and missing values.
* Normalize features (e.g., PM2.5, humidity).
* Perform time-based aggregations (e.g., daily/hourly metrics).
* Implement rolling averages, lag features, and rate-of-change calculations for trend analysis.

#### Expected Output:

* A feature-enhanced dataset that can be used for SQL exploration and ML tasks.

#### Files:

* `Task3/`: Applies transformations such as outlier handling, normalization, and feature engineering.

### Section 3: Spark SQL Exploration & Correlation Analysis

#### Objective:

* Register cleaned data as a temporary view for SQL queries.
* Analyze trends using window functions (ROW\_NUMBER(), LAG(), LEAD()).
* Implement an Air Quality Index (AQI) classification using a UDF-based approach.
* Perform correlation analysis between PM2.5, temperature, and humidity.

#### Expected Output:

* Advanced insights into air quality dynamics, identifying trends and anomalies.

#### Files:

* `Task4/`: Executes Spark SQL queries and applies window functions for trend analysis.

### Section 4: Spark MLlib for Predictive Modeling

#### Objective:

* Feature selection and model preparation for regression (PM2.5 prediction) or classification (AQI categorization).
* Train and evaluate models using Spark MLlib.
* Perform hyperparameter tuning and optimization.
* Prototype real-time prediction integration with the TCP data stream.

#### Expected Output:

* A trained ML model with evaluation metrics and a plan for integrating real-time predictions.

#### Files:

* `Model_Training.py`: Prepares data, trains models, and evaluates performance.
* `Model_Evaluation.py`: Assesses model performance (RMSE, R² for regression or accuracy/F1-score for classification).
* `RealTime_Prediction.py`: Proposes integration of the model into a real-time pipeline.

### Section 5: Pipeline Integration & Dashboard Visualization

#### Objective:

* Integrate all components (ingestion, transformation, SQL analysis, and ML models) into a unified pipeline.
* Develop an interactive dashboard for visualizing air quality trends and predictions.
* Implement real-time alerting and reporting mechanisms.

#### Expected Output:

* A complete pipeline that runs from data ingestion to visualization, with interactive plots and real-time monitoring.

#### Files:

* `dashboard.py`: Implements an interactive dashboard using Plotly or another visualization library.
* Static data and images are saved in the `static/` folder for the dashboard.

## Installation

### Requirements

To install the necessary dependencies, create a Python environment and run the following:

```bash
pip install -r requirements.txt
```

`requirements.txt` includes the following core dependencies:

* `pyspark`
* `pandas`
* `numpy`
* `plotly`
* `seaborn`
* `matplotlib`
* `sklearn`
* `kaleido` (for Plotly image export)
* `requests` (if data is being fetched from an API)

### Setting Up the Environment

* **Apache Spark**: This project assumes you are using Spark for distributed processing. Ensure you have Spark and Hadoop installed or use a managed environment like Databricks.
* **TCP Server Simulation**: A mock TCP server or data source needs to simulate the air quality readings. The project assumes that raw data can be ingested from a TCP stream.

## Running the Project

### Step 1: Data Ingestion & Cleaning

Run `Task1` to simulate the ingestion of raw air quality data. This script will watermark the data to manage late-arriving records.

```bash
python ingestion/Task1/.py
```

Run `Task2` to clean the raw data, validate the schema, and parse timestamps.

```bash
python ingestion/Task2/.py
```

### Step 2: Data Transformation

Run `Task3` to handle outliers, normalize data, and apply time-based aggregations.

```bash
python ingestion/Task3/.py
```

### Step 3: SQL Analysis & Correlation

Run `Task4` to create views, perform SQL analysis, and classify AQI.

```bash
python ingestion/Task4/.py
```

### Step 4: Model Training & Evaluation

Run `Model_Training.py` to train and evaluate regression or classification models.

```bash
python ml_models/Model_Training.py
```

Evaluate the model performance with `Model_Evaluation.py`.

```bash
python ml_models/Model_Evaluation.py
```

### Step 5: Real-Time Prediction Integration

Integrate the model for real-time prediction with `RealTime_Prediction.py`.

```bash
python ml_models/RealTime_Prediction.py
```

### Step 6: Dashboard & Reporting

Run `dashboard.py` to start the interactive dashboard that visualizes the results.

```bash
python dashboard.py
```

## Expected Outputs

1. **Data Ingestion & Cleaning**: Cleaned and validated dataset ready for further analysis.
2. **Data Transformations**: Enhanced dataset with outlier handling, normalized features, and time-based aggregations.
3. **SQL Analysis**: Query results identifying pollution hotspots, trends, and AQI classifications.
4. **ML Model**: A trained model with performance metrics (RMSE, R², accuracy, etc.).
5. **Interactive Dashboard**: A dynamic dashboard with time-series charts, AQI breakdowns, and correlation heatmaps.

## Conclusion

This project implements a robust pipeline for air quality monitoring and forecasting. By integrating data ingestion, transformation, SQL analysis, and machine learning, the system can provide real-time insights into air pollution levels, predict future conditions, and support data-driven decision-making.
