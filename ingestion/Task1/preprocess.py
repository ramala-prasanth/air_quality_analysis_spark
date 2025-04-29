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



# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_timestamp, expr, trim
# from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# # 1. Start Spark Session
# spark = SparkSession.builder \
#     .appName("AirQualityStreamingUpdated") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # 2. Read raw stream from TCP
# raw_stream = spark.readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 9999) \
#     .load()

# # 3. Print raw data to the console for debugging
# # # raw_stream.printSchema()  # Print schema to see the structure of the raw data
# # raw_stream.writeStream \
# #     .outputMode("append") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()

# # 4. Parse CSV columns
# from pyspark.sql.functions import col, trim, regexp_replace



# parsed = raw_stream.selectExpr("split(value, ',') as cols") \
#     .filter("size(cols) = 9") \
#     .filter(
#         (~col("cols").getItem(0).contains("location_id")) &
#         (col("cols").getItem(1) != "sensors_id") &
#         (col("cols").getItem(2) != "location") &
#         (col("cols").getItem(3) != "datetime") &
#         (col("cols").getItem(4) != "lat") &
#         (col("cols").getItem(5) != "lon") &
#         (col("cols").getItem(6) != "parameter") &
#         (col("cols").getItem(7) != "units") &
#         (col("cols").getItem(8) != "value")
#     ) \
#     .select(
#         regexp_replace(col("cols").getItem(0), r"[\'\[\]]", "").alias("location_id"),
#         regexp_replace(col("cols").getItem(1), r"[\'\[\]]", "").alias("sensors_id"),
#         regexp_replace(col("cols").getItem(2), r"[\'\[\]]", "").alias("location"),
#         regexp_replace(col("cols").getItem(3), r"[\'\[\]]", "").alias("datetime"),
#         regexp_replace(col("cols").getItem(4), r"[\'\[\]]", "").cast("double").alias("lat"),
#         regexp_replace(col("cols").getItem(5), r"[\'\[\]]", "").cast("double").alias("lon"),
#         regexp_replace(col("cols").getItem(6), r"[\'\[\]]", "").alias("parameter"),
#         regexp_replace(col("cols").getItem(7), r"[\'\[\]]", "").alias("units"),
#         regexp_replace(col("cols").getItem(8), r"[\'\[\]]", "").cast("double").alias("value")
#     )



# # # 5. Print parsed data (after splitting columns) to the console for debugging
# # parsed.writeStream \
# #     .outputMode("append") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()

# # 6. Convert to timestamp and add watermark
# with_timestamp = parsed.withColumn("timestamp", to_timestamp("datetime")) \
#     .withWatermark("timestamp", "10 minutes")


# # # 7. Print data with timestamp and watermark to the console for debugging
# # with_timestamp.writeStream \
# #     .outputMode("append") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .start()



# # 8. Pivot metrics (parameter → PM2.5, temp, humidity, etc.)
# from pyspark.sql.functions import when

# # Aggregate each metric separately using conditional logic
# from pyspark.sql.functions import expr
# from pyspark.sql import functions as F

# clean_df = with_timestamp.groupBy(
#     F.col("datetime").alias("timestamp"),  # datetime column to timestamp
#     F.col("location_id"),
#     F.col("location")
# ).agg(
#     F.first(F.when(F.lower(F.col("parameter")) == "pm25", F.col("value"))).alias("PM2_5"),
#     F.first(F.when(F.lower(F.col("parameter")) == "temperature", F.col("value"))).alias("temperature"),
#     F.first(F.when(F.lower(F.col("parameter")) == "humidity", F.col("value"))).alias("humidity")
# )

# # clean_df.show(truncate=False)

# # clean_df = with_timestamp.groupBy(
# #     col("timestamp"), col("location_id"), col("location")
# # ).agg(
# #     expr("first(case when lower(parameter) = 'pm25' then value end)").alias("PM2_5"),
# #     expr("first(case when lower(parameter) = 'temperature' then value end)").alias("temperature"),
# #     expr("first(case when lower(parameter) = 'humidity' then value end)").alias("humidity")
# # )


# # 9. Print the final cleaned data to the console for debugging
# clean_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# # 10. Output to CSV
# # csv_query = clean_df.writeStream \
# #     .outputMode("append") \
# #     .format("csv") \
# #     .option("path", "ingestion/output1") \
# #     .option("checkpointLocation", "ingestion/checkpoint1") \
# #     .option("header", "true") \
# #     .start()

# csv_query = with_timestamp.writeStream \
#     .outputMode("append") \
#     .format("csv") \
#     .option("path", "ingestion/output1") \
#     .option("checkpointLocation", "ingestion/checkpoint1") \
#     .start()


# # Await termination of all queries
# csv_query.awaitTermination()
