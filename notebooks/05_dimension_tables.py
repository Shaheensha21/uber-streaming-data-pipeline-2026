from pyspark.sql.functions import col, year, month, dayofmonth, hour

# Read data
df_static = spark.read.format("delta") \
    .load("abfss://bronze@uberdatalake2002.dfs.core.windows.net/uber-final")

# DIM_DRIVER
df_static.select("driver_id").distinct() \
    .write.format("delta") \
    .mode("overwrite") \
    .save("abfss://bronze@uberdatalake2002.dfs.core.windows.net/dim_driver")

# DIM_LOCATION
df_static.select("location").distinct() \
    .write.format("delta") \
    .mode("overwrite") \
    .save("abfss://bronze@uberdatalake2002.dfs.core.windows.net/dim_location")

# DIM_TIME
df_time = df_static.withColumn("event_time", col("timestamp").cast("timestamp")) \
    .select("event_time") \
    .withColumn("year", year("event_time")) \
    .withColumn("month", month("event_time")) \
    .withColumn("day", dayofmonth("event_time")) \
    .withColumn("hour", hour("event_time"))

df_time.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://bronze@uberdatalake2002.dfs.core.windows.net/dim_time")