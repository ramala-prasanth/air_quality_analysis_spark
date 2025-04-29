import pandas as pd

# === Step 1: Read the datasets ===

# Read weather data
weather_df = pd.read_csv("ingestion/Task1/output/weather.csv")
weather_df["timestamp"] = pd.to_datetime(weather_df["timestamp"], utc=True)
weather_df["location"] = weather_df["location"].str.replace(" ", "", regex=False)

# Read sensor data
sensor_df = pd.read_csv("ingestion/Task1/output/outputf/part-00000-f1e0e91e-0aca-4247-a34d-bb4d788d0c7f-c000.csv")
sensor_df["timestamp"] = pd.to_datetime(sensor_df["timestamp"], utc=True)  # ✅ Convert to same tz
sensor_df["location"] = sensor_df["location"].str.replace("USDiplomaticPost:", "", regex=False)



# === Step 2: Pivot sensor data to merge metrics (pm25, temperature, humidity) ===

pivot_sensor_df = sensor_df.pivot_table(
    index=["location", "timestamp"],
    columns="parameter",
    values="value",
    aggfunc="first"
).reset_index()

# Rename columns (optional, cleaner)
pivot_sensor_df.columns.name = None

# === Step 3: Merge weather data with sensor data ===

merged_df = pd.merge(
    pivot_sensor_df,
    weather_df,
    on=["location", "timestamp"],
    how="outer",
    suffixes=("_sensor", "_weather")
)

# === Step 4: Prioritize sensor values over weather ===

merged_df["temperature"] = merged_df["temperature_sensor"].combine_first(merged_df["temperature_weather"])
merged_df["humidity"] = merged_df["humidity_sensor"].combine_first(merged_df["humidity_weather"])

# Select only final useful columns
final_df = merged_df[["location", "timestamp", "pm25", "temperature", "humidity"]]

# === Step 5: Validation ===

print("\n=== Summary Statistics ===")
print(final_df.describe(include='all'))

print("\n=== Missing Values ===")
print(final_df.isnull().sum())

print("\n=== Sample Records ===")
print(final_df.sample(5, random_state=42))

# === Step 6: Save output ===

final_df.sort_values(by=["location", "timestamp"], inplace=True)
final_df.to_csv("clean_merged_data.csv", index=False)

print("\n✅ Clean merged data saved to 'clean_merged_data.csv'")
