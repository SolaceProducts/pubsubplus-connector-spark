import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, unix_timestamp, unix_millis, unix_seconds

# Initialize Spark session
spark = SparkSession.builder.appName("SolaceSparkStreamingIntegrationTest").getOrCreate()

# Define logger
logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# Define stream name
stream_name = "solace-spark-connector-integration-test"
out_stream_name = "/opt/spark/checkpoint/solace-spark-connector-integration-test-checkpoint"

# Read all environment variables
env_vars = dict(os.environ)  # everything present in env

# if only write
only_write = env_vars.get("only_write", "false")
# Extract add_columns
add_columns = env_vars.get("add_columns", "")
add_columns_list = [col.strip() for col in add_columns.split(",") if col.strip()]
# Extract drop_columns
drop_columns = env_vars.get("drop_columns", "")
drop_columns_list = [col.strip() for col in drop_columns.split(",") if col.strip()]
# Filter for only those starting with 'SOLACE_'
solace_env = {k[len("solace_"):]: v for k, v in env_vars.items() if k.startswith("solace_")}

def write_each_batch(batch_df, batch_id):
    batch_df_cached = batch_df.cache()
    # Drop columns if provided
    if drop_columns_list:
        batch_df_cached = batch_df_cached.drop(*drop_columns_list)
    if add_columns_list:
        for col_name in add_columns_list:
            if col_name == "timestamp_ms":
                batch_df_cached = batch_df_cached.withColumn(
                    "TimeStamp",
                    unix_millis(current_timestamp())
                )
            elif col_name == "timestamp_sec":
                batch_df_cached = batch_df_cached.withColumn(
                    "TimeStamp",
                    unix_seconds(current_timestamp())
                )
    count = batch_df_cached.count()
    logger.info(f"Write Batch {batch_id} count: {count}")
    batch_writer = batch_df_cached.write \
        .format("solace") \
        .mode("append")

    # Add options dynamically
    for k, v in write_options.items():
        print(f"Key {k} Value {v}")
        batch_writer = batch_writer.option(k, v)

    batch_writer.save()

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
    "queue.receiveWaitTimeout": 1000,
    "includeHeaders": True
}

read_options = {**common_options, **read_options}

write_options = {
    "topic": "test/topic",
}

write_options = {**common_options, **write_options}


# Apply overrides + deletions
for key, value in solace_env.items():
    normalized_key = key.strip()

    if str(value) == "__DELETE__":
        if only_write == "false":
            if normalized_key in read_options:
                logger.info(f"Removing option: {normalized_key}")
                del read_options[normalized_key]
        if normalized_key in write_options:
            logger.info(f"Removing option: {normalized_key}")
            del write_options[normalized_key]
    elif str(value) == "NULL":
        if only_write == "false":
            if normalized_key in read_options:
                read_options[normalized_key] = None
        if normalized_key in write_options:
            write_options[normalized_key] = None
    elif "lvq_topic" in normalized_key:
        read_options["lvq.topic"] = value
    elif "lvq_name" in normalized_key:
        read_options["lvq.name"] = value
    else:
        if only_write == "false":
            if normalized_key in read_options:
                logger.info(f"Overriding {normalized_key}: {read_options[normalized_key]} -> {value}")
            else:
                logger.info(f"Adding new option: {normalized_key} = {value}")
            read_options[normalized_key] = value

        if normalized_key in write_options:
            logger.info(f"Overriding {normalized_key}: {write_options[normalized_key]} -> {value}")
        else:
            logger.info(f"Adding new option: {normalized_key} = {value}")
        write_options[normalized_key] = value

print(f"Read options {read_options}")
print(f"Write options {write_options}")
# Apply final options to reader
reader = spark.readStream.format("solace")
for k, v in read_options.items():
    reader = reader.option(k, v)

df = reader.load().drop("TimeStamp")

query = df.writeStream.foreachBatch(write_each_batch) \
    .start()

spark.streams.awaitAnyTermination()
