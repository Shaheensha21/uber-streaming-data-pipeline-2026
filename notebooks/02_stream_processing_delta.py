from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# Schema
ride_schema = StructType([
    StructField("ride_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Event Hub connection
connection_string = "Endpoint=sb://uber-eventhub-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY;EntityPath=uber-rides"

eh_conf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
    'eventhubs.consumerGroup': "$Default"
}

# Read stream
df_stream = spark.readStream.format("eventhubs").options(**eh_conf).load()

# Parse JSON
df_parsed = df_stream \
    .withColumn("jsonData", col("body").cast("string")) \
    .withColumn("ride", from_json(col("jsonData"), ride_schema)) \
    .select("ride.*")

# Write to Delta (Bronze/Silver)
query = df_parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation", "abfss://bronze@uberdatalake2002.dfs.core.windows.net/checkpoint_main") \
    .outputMode("append") \
    .start("abfss://bronze@uberdatalake2002.dfs.core.windows.net/uber-final")

query.awaitTermination()