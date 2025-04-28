from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("AirQualityMonitoringSection1") \
    .getOrCreate()

# Step 2: Read merged_file.csv directly
df = spark.read.csv("merged_file.csv", header=True, inferSchema=True)

# Step 3: Clean and preprocess
# - Convert datetime to TimestampType
# - Ensure lat, lon, value are double
df = df.withColumn("datetime", to_timestamp(col("datetime"))) \
       .withColumn("lat", col("lat").cast(DoubleType())) \
       .withColumn("lon", col("lon").cast(DoubleType())) \
       .withColumn("value", col("value").cast(DoubleType()))

# Step 4: Basic Cleaning
# - Drop rows where essential columns are NULL
# - Drop duplicate rows
clean_df = df.dropna(subset=["datetime", "location", "lat", "lon", "parameter", "value"]) \
             .dropDuplicates()

# Step 5: Cross-Verify Data Quality

print("\n=== Data Summary Statistics ===")
# Overall counts
print(f"Total Rows: {clean_df.count()}")
print(f"Unique Locations: {clean_df.select('location').distinct().count()}")
print(f"Unique Parameters: {clean_df.select('parameter').distinct().count()}")

# Min and Max Value Checks
print("\nMin and Max of 'value' column:")
clean_df.select("value").summary("min", "max").show()

# Sample Data Preview
print("\nSample 5 rows:")
clean_df.show(5, truncate=False)

# Step 6: Pivot Metrics (pm25, temp, humidity etc.)
pivoted_df = clean_df.groupBy("datetime", "location", "lat", "lon") \
    .pivot("parameter", ["pm25"]) \
    .agg({"value": "avg"})

# Step 7: Write the cleaned merged data to CSV
pivoted_df.write.csv("output/section1_csv", header=True, mode="overwrite")
