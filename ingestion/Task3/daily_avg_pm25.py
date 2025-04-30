
from common_setup import spark

result_df=spark.sql("""
    SELECT
        location,
        date,
        AVG(pm25) AS avg_pm25
    FROM global_temp.air_quality
    GROUP BY location, date
    ORDER BY location, date
""")

result_df.write.csv("Outputs/daily_avg_pm25", header=True, mode="overwrite")
