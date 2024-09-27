package com.solacecoe.connectors.spark.streaming.properties;

public class SolaceSparkStreamingProperties {
    public static String HOST = "host";
    public static String VPN = "vpn";
    public static String USERNAME = "username";
    public static String PASSWORD = "password";
    public static String QUEUE = "queue";
    public static String SOLACE_CONNECT_RETRIES = "connectRetries";
    public static String SOLACE_RECONNECT_RETRIES = "reconnectRetries";
    public static String SOLACE_CONNECT_RETRIES_PER_HOST = "connectRetriesPerHost";
    public static String SOLACE_RECONNECT_RETRIES_WAIT_TIME = "reconnectRetryWaitInMillis";
    public static String SOLACE_API_PROPERTIES_PREFIX = "solace.apiProperties.";

    public static String BATCH_SIZE = "batchSize";
    public static String BATCH_SIZE_DEFAULT = "1";
    public static String ACK_LAST_PROCESSED_MESSAGES = "ackLastProcessedMessages";
    public static String ACK_LAST_PROCESSED_MESSAGES_DEFAULT = "false";
    public static String SKIP_DUPLICATES = "skipDuplicates";
    public static String SKIP_DUPLICATES_DEFAULT = "false";
    public static String INCLUDE_HEADERS = "includeHeaders";
    public static String INCLUDE_HEADERS_DEFAULT = "false";
    public static String PARTITIONS = "partitions";
    public static String PARTITIONS_DEFAULT = "1";
    public static String OFFSET_INDICATOR = "offsetIndicator";
    public static String OFFSET_INDICATOR_DEFAULT = "MESSAGE_ID";

    public static String OAUTH_CLIENT_AUTHSERVER_URL = "solace.oauth.client.auth-server-url";
    public static String OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE = "solace.oauth.client.auth-server.client-certificate.file";
    public static String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_FILE = "solace.oauth.client.auth-server.truststore.file";
    public static String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD = "solace.oauth.client.auth-server.truststore.password";
    public static String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_TYPE = "solace.oauth.client.auth-server.truststore.type";
    public static String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_TYPE_DEFAULT = "JKS";
    public static String OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE = "solace.oauth.client.auth-server.ssl.validate-certificate";
    public static String OAUTH_CLIENT_AUTHSERVER_TLS_VERSION = "solace.oauth.client.auth-server.tls.version";
    public static String OAUTH_CLIENT_AUTHSERVER_TLS_VERSION_DEFAULT = "TLSv1.2";
    public static String OAUTH_CLIENT_CLIENT_ID = "solace.oauth.client.client-id";
    public static String OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET = "solace.oauth.client.credentials.client-secret";
    public static String OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL = "solace.oauth.client.token.refresh.interval";
    public static String OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL_DEFAULT = "60";
    public static String OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT = "solace.oauth.client.token.fetch.timeout";
    public static String OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT_DEFAULT = "100";

    public static String SOLACE_SPARK_CONNECTOR_LVQ_NAME = "solace.lvq.name";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_TOPIC = "solace.lvq.topic";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_NAME = "solace.spark.connector.state";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC = "solace/spark/connector/offset";

    public static String CHECKPOINT_TYPE = "checkpoint.type";
    public static String CHECKPOINT_TYPE_DEFAULT = "lvq";
    public static String LAST_SUCCESSFUL_MESSAGE_ID = "lastSuccessfulMessageId";
    public static String LAST_SUCCESSFUL_MESSAGE_ID_THRESHOLD = "lastSuccessfulMessageIdThreshold";;
}
