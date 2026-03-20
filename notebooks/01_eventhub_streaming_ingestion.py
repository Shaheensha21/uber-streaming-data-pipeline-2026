from pyspark.sql.functions import col

# Event Hub connection string (REPLACE YOUR KEY)
connection_string = "Endpoint=sb://uber-eventhub-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY;EntityPath="

# Encrypt connection string
eh_conf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
    'eventhubs.consumerGroup': "$Default"
}

# Read stream from Event Hub
df_stream = spark.readStream \
    .format("eventhubs") \
    .options(**eh_conf) \
    .load()

# Convert body to string for debugging
df_debug = df_stream.withColumn("body_str", col("body").cast("string"))

# Display live data (for testing only)
display(df_debug, checkpointLocation="abfss://bronze@uberdatalake2002.dfs.core.windows.net/checkpoint_display")
