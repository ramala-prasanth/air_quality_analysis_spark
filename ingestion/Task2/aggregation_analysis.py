import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

# Load the dataset
df = pd.read_csv("ingestion/Task2/task2_input.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

# ---------------------------
# 1. HANDLE OUTLIERS & MISSING VALUES
# ---------------------------

# Define outlier caps based on domain knowledge (example values, adjust as needed)
pm25_cap = 500
temperature_cap = 60
humidity_cap = 100

# Cap values
df["pm25"] = df["pm25"].clip(upper=pm25_cap)
df["temperature"] = df["temperature"].clip(upper=temperature_cap)
df["humidity"] = df["humidity"].clip(upper=humidity_cap)

# Impute missing values using rolling median (for time-aware data)
df = df.sort_values("timestamp")

for col in ["pm25", "temperature", "humidity"]:
    df[col] = df[col].fillna(df[col].rolling(window=5, min_periods=1, center=True).median())

# ---------------------------
# 2. NORMALIZE / STANDARDIZE FEATURES
# ---------------------------

scaler = StandardScaler()
df[["pm25_scaled", "temperature_scaled", "humidity_scaled"]] = scaler.fit_transform(
    df[["pm25", "temperature", "humidity"]]
)

# ---------------------------
# 3. DAILY / HOURLY AGGREGATIONS
# ---------------------------

# Create date and hour columns
df["date"] = df["timestamp"].dt.date
df["hour"] = df["timestamp"].dt.hour

# Hourly aggregation
hourly_agg = df.groupby(["location", "date", "hour"]).agg({
    "pm25": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index().rename(columns={
    "pm25": "avg_pm25",
    "temperature": "avg_temperature",
    "humidity": "avg_humidity"
})

# Daily aggregation
daily_agg = df.groupby(["location", "date"]).agg({
    "pm25": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index().rename(columns={
    "pm25": "daily_avg_pm25",
    "temperature": "daily_avg_temperature",
    "humidity": "daily_avg_humidity"
})

# ---------------------------
# 4. ROLLING AVERAGES, LAG FEATURES, RATE OF CHANGE
# ---------------------------

# Sort data
df = df.sort_values(["location", "timestamp"])

# Rolling averages (window = 3 hours)
df["pm25_roll_avg_3"] = df.groupby("location")["pm25"].transform(lambda x: x.rolling(window=3, min_periods=1).mean())
df["temperature_roll_avg_3"] = df.groupby("location")["temperature"].transform(lambda x: x.rolling(window=3, min_periods=1).mean())

# Lag features
df["pm25_lag_1"] = df.groupby("location")["pm25"].shift(1)
df["temperature_lag_1"] = df.groupby("location")["temperature"].shift(1)

# Rate of change
df["pm25_rate_of_change"] = df["pm25"] - df["pm25_lag_1"]
df["temperature_rate_of_change"] = df["temperature"] - df["temperature_lag_1"]

# ---------------------------
# 5. SAVE FINAL DATASETS
# ---------------------------

# Save enriched dataset
df.to_csv("ingestion/Task2/processed_data/feature_enhanced.csv", index=False)

# Save aggregations
hourly_agg.to_csv("ingestion/Task2/processed_data/hourly_aggregation.csv", index=False)
daily_agg.to_csv("ingestion/Task2/processed_data/daily_aggregation.csv", index=False)