from pyspark.sql.functions import col

# Read Delta as batch
df_static = spark.read.format("delta") \
    .load("abfss://bronze@uberdatalake2002.dfs.core.windows.net/uber-final")

# Create fact table
df_fact = df_static.select(
    "ride_id",
    "driver_id",
    "location",
    col("timestamp").cast("timestamp").alias("event_time"),
    "fare"
)

# Save fact table
df_fact.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://bronze@uberdatalake2002.dfs.core.windows.net/fact_rides")