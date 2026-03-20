from pyspark.sql.functions import window, col

# Read from Delta (batch → convert to streaming)
df = spark.read.format("delta") \
    .load("abfss://bronze@uberdatalake2002.dfs.core.windows.net/uber-final")

# Convert to streaming DataFrame
df_stream = df.selectExpr("*")

# Window aggregation
df_window = df_stream \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .groupBy(window("event_time", "1 minute")) \
    .count()

# Write result
df_window.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "abfss://bronze@uberdatalake2002.dfs.core.windows.net/checkpoint_window") \
    .start("abfss://bronze@uberdatalake2002.dfs.core.windows.net/window-output")