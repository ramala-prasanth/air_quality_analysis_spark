from common_setup import spark

result_df=spark.sql("""
WITH trend AS (
  SELECT *,
         LAG(pm25, 1) OVER (PARTITION BY location ORDER BY timestamp) AS pm25_lag1,
         LAG(pm25, 2) OVER (PARTITION BY location ORDER BY timestamp) AS pm25_lag2
        FROM global_temp.air_quality
        )
        SELECT location, timestamp, pm25, pm25_lag1, pm25_lag2
        FROM trend
        WHERE pm25_lag1 IS NOT NULL AND pm25_lag2 IS NOT NULL
        AND pm25_lag2 < pm25_lag1 AND pm25_lag1 < pm25
        ORDER BY location, timestamp

""")

result_df.write.csv("Outputs/ConsecutiveIncreases", header=True, mode="overwrite")

res_df = spark.sql("""
WITH changes AS (
  SELECT *,
         LAG(pm25, 1) OVER (PARTITION BY location ORDER BY timestamp) AS prev_pm25
  FROM global_temp.air_quality
)
SELECT location, timestamp, pm25, prev_pm25, (pm25 - prev_pm25) AS change
FROM changes
WHERE prev_pm25 IS NOT NULL
ORDER BY ABS(pm25 - prev_pm25) DESC
LIMIT 10
""")

res_df.write.csv("Outputs/anomalies_analysis", header=True, mode="overwrite")

outliers_df = spark.sql("""
WITH anomaly_check AS (
  SELECT *,
         LAG(pm25, 1) OVER (PARTITION BY location ORDER BY timestamp) AS prev_pm25,
         LEAD(pm25, 1) OVER (PARTITION BY location ORDER BY timestamp) AS next_pm25
  FROM global_temp.air_quality
)
SELECT location, timestamp, prev_pm25, pm25, next_pm25
FROM anomaly_check
WHERE prev_pm25 IS NOT NULL AND next_pm25 IS NOT NULL
  AND (pm25 - prev_pm25 >= 80 AND pm25 - next_pm25 >= 80)
ORDER BY timestamp
""")

outliers_df.write.csv("Outputs/outliers", header=True, mode="overwrite")


row_changes= spark.sql("""
                       SELECT location, timestamp, pm25,
       ROW_NUMBER() OVER (PARTITION BY location ORDER BY timestamp) AS seq_num
FROM global_temp.air_quality
ORDER BY location, timestamp
""")

outliers_df.write.csv("Outputs/row_changes", header=True, mode="overwrite")

