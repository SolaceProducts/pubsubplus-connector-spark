import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, unix_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("SolaceSparkStreamingIntegrationTest").config("spark.task.maxFailures", "1").config("spark.streaming.stopGracefullyOnShutdown", "true").config("spark.sql.streaming.stopTimeout", "10000").getOrCreate()

# Define logger
logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# Define stream name
stream_name = "solace-spark-connector-integration-test"
out_stream_name = "/opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint"

# Read all environment variables
env_vars = dict(os.environ)  # everything present in env

# Filter for only those starting with 'SOLACE_'
solace_env = {k[len("solace_"):]: v for k, v in env_vars.items() if k.startswith("solace_")}

def write_each_batch(batch_df, batch_id):
    batch_df_cached = batch_df.cache()
    empty = batch_df_cached.isEmpty()
    # Check if batch has any rows
    if not empty:
        # Get first row's Payload as string
        first_row = batch_df_cached.select("Payload").head()  # returns Row object
        print(f"Batch Id after header {batch_id}")
        payload_value = first_row["Payload"]              # access column value

        # If Payload is binary or bytearray, convert to bytes then decode
        if isinstance(payload_value, (bytes, bytearray)):
            payload_str = bytes(payload_value).decode("utf-8")
        else:
            payload_str = str(payload_value)  # fallback for string type

        count = batch_df_cached.count()
        print(f"Write Batch {batch_id} count: {count}")
        logger.info(f"Write Batch {batch_id} count: {count}")

        print(f"Write Payload is: {payload_str}")
        logger.info(f"Write Payload is: {payload_str}")

# Base options
options = {
    "host": "tcp://solace-broker:55555",
    "vpn": "default",
    "username": "root",
    "password": "password",
    "queue": "Solace/Queue/0",
    "connectRetries": 2,
    "reconnectRetries": 2,
    "batchSize": 50,
    "queue.receiveWaitTimeout": 1000
}


# Apply overrides + deletions
for key, value in solace_env.items():
    normalized_key = key.strip()

    if str(value) == "__DELETE__":
        if normalized_key in options:
            print(f"Removing option: {normalized_key}")
            del options[normalized_key]
    elif str(value) == "NULL":
        if normalized_key in options:
            options[normalized_key] = None
    elif "lvq_topic" in normalized_key:
        options["lvq.topic"] = value
    elif "lvq_name" in normalized_key:
        options["lvq.name"] = value
    else:
        if normalized_key in options:
            print(f"Overriding {normalized_key}: {options[normalized_key]} -> {value}")
        else:
            print(f"Adding new option: {normalized_key} = {value}")
        options[normalized_key] = value

# Apply final options to reader
reader = spark.readStream.format("solace")
for k, v in options.items():
    reader = reader.option(k, v)

df = reader.load().drop("TimeStamp")

query = df \
    .writeStream \
        .queryName(out_stream_name) \
        .outputMode("append") \
        .foreachBatch(write_each_batch) \
        .option("checkpointLocation", f"{out_stream_name}") \
        .start()

spark.streams.awaitAnyTermination()
