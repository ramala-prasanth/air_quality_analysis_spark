from common_setup import spark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

df = spark.sql("SELECT * FROM global_temp.air_quality")

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

result_df = df.withColumn("AQI_Category", aqi_udf(df["pm25"])) \
              .groupBy("AQI_Category") \
              .count()

result_df.write.csv("Outputs/aqi_classification", header=True, mode="overwrite")
