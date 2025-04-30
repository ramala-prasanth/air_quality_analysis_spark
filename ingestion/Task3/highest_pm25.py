from common_setup import spark

result_df=spark.sql("""
    SELECT location, AVG(pm25) AS avg_pm25
    FROM global_temp.air_quality
    WHERE timestamp >= (
        SELECT MAX(timestamp) FROM global_temp.air_quality
    ) - INTERVAL 1 DAY
    GROUP BY location
    ORDER BY avg_pm25 DESC
""")

result_df.write.csv("Outputs/highest_pm25.csv", header=True, mode="overwrite")
