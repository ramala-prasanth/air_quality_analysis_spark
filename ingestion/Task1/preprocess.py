from pyspark.sql import SparkSession, DataFrame

# Initialize Spark Session
spark: SparkSession = SparkSession.builder.appName("AirQualityAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load DataFrame from multiple CSV files
df: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("output/air_quality_csv")

# Function to explore and clean DataFrame
def explore(df: DataFrame) -> DataFrame:
    df = df.na.drop(subset=["value"])
    df.show(truncate=False)
    return df

# Save cleaned and explored data into a single output CSV
explore(df).coalesce(1).write.option("header", "true").mode("overwrite").csv("output/preprocessed_csv")
