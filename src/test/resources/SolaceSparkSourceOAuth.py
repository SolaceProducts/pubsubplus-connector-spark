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

# Read if oauth should connect to insecure port
is_oauth_insecure = env_vars.get("oauth_insecure", "false")
add_invalid_oauth_url = env_vars.get("add_invalid_oauth_url", "false")
add_invalid_tls = env_vars.get("add_invalid_tls", "false")
add_client_cert = env_vars.get("add_client_cert", "false")
add_client_cert_to_custom_truststore = env_vars.get("add_client_cert_to_custom_truststore", "false")
add_access_token_file = env_vars.get("add_access_token_file", "false")
set_truststore_password_null = env_vars.get("set_truststore_password_null", "false")

unset_oauth_url = env_vars.get("unset_oauth_url", "false")
set_oauth_url_empty = env_vars.get("set_oauth_url_empty", "false")

unset_oauth_client_id = env_vars.get("unset_oauth_client_id", "false")
set_oauth_client_id_empty = env_vars.get("set_oauth_client_id_empty", "false")

unset_oauth_client_secret = env_vars.get("unset_oauth_client_secret", "false")
set_oauth_client_secret_empty = env_vars.get("set_oauth_client_secret_empty", "false")

set_access_token_file_empty = env_vars.get("set_access_token_file_empty", "false")

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
    "host": "tcps://solace-broker:55443",
    "vpn": "default",
    "username": "user",
    "password": "pass",
    "queue": "integration-test-queue",
    "connectRetries": 2,
    "reconnectRetries": 2,
    "batchSize": 50,
    "queue.receiveWaitTimeout": 1000,
    "solace.apiProperties.AUTHENTICATION_SCHEME": "AUTHENTICATION_SCHEME_OAUTH2",
    "solace.apiProperties.SSL_VALIDATE_CERTIFICATE": False,
    "solace.oauth.client.client-id": "solace",
    "solace.oauth.client.credentials.client-secret": "solace-secret",
    "solace.oauth.client.auth-server.ssl.validate-certificate": False,
    "solace.oauth.client.auth-server-url": "https://keycloak:8443/realms/solace/protocol/openid-connect/token"
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
    elif "_" in normalized_key:
        normalized_key = normalized_key.replace("_", ".")
        options[normalized_key] = value
    else:
        if normalized_key in options:
            print(f"Overriding {normalized_key}: {options[normalized_key]} -> {value}")
        else:
            print(f"Adding new option: {normalized_key} = {value}")
        options[normalized_key] = value
if is_oauth_insecure == "true":
    options["solace.oauth.client.auth-server-url"] = "http://keycloak:8080/realms/solace/protocol/openid-connect/token"

if add_client_cert == "true":
    options["solace.oauth.client.auth-server.client-certificate.file"] = "/opt/spark/work-dir/keycloak.crt"
    if set_truststore_password_null == "false":
        options["solace.oauth.client.auth-server.truststore.password"] = "changeit"
    options["solace.oauth.client.auth-server.ssl.validate-certificate"] = "true"

if add_client_cert_to_custom_truststore == "true":
    options["solace.oauth.client.auth-server.truststore.file"] = "/opt/spark/work-dir/custom_truststore.jks"

if add_access_token_file == "true":
    options["solace.oauth.client.access-token"] = "/opt/spark/work-dir/accesstoken.txt"
    del options["solace.oauth.client.client-id"]
    del options["solace.oauth.client.credentials.client-secret"]
    del options["solace.oauth.client.auth-server.ssl.validate-certificate"]
    del options["solace.oauth.client.auth-server-url"]

if add_invalid_oauth_url == "true":
    options["solace.oauth.client.auth-server-url"] = "http://keycloak:8080/realms/invalid/protocol/openid-connect/token"

if add_invalid_tls == "true":
    options["solace.oauth.client.auth-server.tls.version"] = "invalid"

if unset_oauth_url == "true":
    del options["solace.oauth.client.auth-server-url"]

if set_oauth_url_empty == "true":
    options["solace.oauth.client.auth-server-url"] = ""

if unset_oauth_client_id == "true":
    del options["solace.oauth.client.client-id"]

if set_oauth_client_id_empty == "true":
    options["solace.oauth.client.client-id"] = ""

if unset_oauth_client_secret == "true":
    del options["solace.oauth.client.credentials.client-secret"]

if set_oauth_client_secret_empty == "true":
    options["solace.oauth.client.credentials.client-secret"] = ""

if set_access_token_file_empty == "true":
    options["solace.oauth.client.access-token"] = ""
# Apply final options to reader
print(f"Read options {options}")
reader = spark.readStream.format("solace")
for k, v in options.items():
    reader = reader.option(k, v)

df = reader.load().drop("TimeStamp")

writer = df \
    .writeStream \
        .queryName(out_stream_name) \
        .option("checkpointLocation", f"{out_stream_name}") \
        .option("id", "my-default-id") \
        .option("topic", "random/topic") \
        .format("solace")

for k, v in options.items():
    writer = writer.option(k, v)

query = writer.start()
spark.streams.awaitAnyTermination()
