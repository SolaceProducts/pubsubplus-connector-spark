import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, unix_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("SolaceSparkStreamingIntegrationTest").getOrCreate()

# Define logger
logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# Define stream name
stream_name = "solace-spark-connector-integration-test"
out_stream_name = "/opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint"

# Read all environment variables
env_vars = dict(os.environ)  # everything present in env

# Filter for only those starting with 'SOLACE_'
solace_env = {k[len("solace_"):]: v for k, v in env_vars.items() if k.startswith("solace_")}

# Base options
common_options = {
    "host": "tcp://solace-broker:55555",
    "vpn": "default",
    "username": "root",
    "password": "password",
    "connectRetries": 2,
    "reconnectRetries": 2,
    "batchSize": 50,
}
read_options = {
    "queue": "Solace/Queue/0",
    "queue.receiveWaitTimeout": 1000
}

read_options = {**common_options, **read_options}

write_options = {
    "id": "test-id",
    "topic": "test/topic"
}

write_options = {**common_options, **write_options}


# Apply overrides + deletions
for key, value in solace_env.items():
    normalized_key = key.strip()

    if str(value) == "__DELETE__":
        if normalized_key in read_options:
            print(f"Removing option: {normalized_key}")
            del read_options[normalized_key]
        if normalized_key in write_options:
            print(f"Removing option: {normalized_key}")
            del write_options[normalized_key]
    elif str(value) == "NULL":
        if normalized_key in read_options:
            read_options[normalized_key] = None
        if normalized_key in write_options:
            write_options[normalized_key] = None
    elif "lvq_topic" in normalized_key:
        read_options["lvq.topic"] = value
    elif "lvq_name" in normalized_key:
        read_options["lvq.name"] = value
    else:
        if normalized_key in read_options:
            print(f"Overriding {normalized_key}: {read_options[normalized_key]} -> {value}")
        else:
            print(f"Adding new option: {normalized_key} = {value}")
        read_options[normalized_key] = value

        if normalized_key in write_options:
            print(f"Overriding {normalized_key}: {write_options[normalized_key]} -> {value}")
        else:
            print(f"Adding new option: {normalized_key} = {value}")
        write_options[normalized_key] = value

# Apply final options to reader
reader = spark.readStream.format("solace")
for k, v in read_options.items():
    reader = reader.option(k, v)

df = reader.load().drop("TimeStamp")

writer = df.writeStream.format("solace").option("checkpointLocation", f"{out_stream_name}")
for k, v in write_options.items():
    writer = writer.option(k, v)
query = writer.start()

spark.streams.awaitAnyTermination()
