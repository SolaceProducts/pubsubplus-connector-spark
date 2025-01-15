package com.solacecoe.connectors.spark.streaming.properties;

public class SolaceSparkStreamingProperties {
    public static final String HOST = "host";
    public static final String VPN = "vpn";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String QUEUE = "queue";
    public static final String TOPIC = "topic";
    public static final String MESSAGE_ID = "id";
    public static final String BATCH_SIZE = "batchSize";
    public static final String BATCH_SIZE_DEFAULT = "1";
    public static final String REPLAY_STRATEGY = "replayStrategy";
    public static final String REPLAY_STRATEGY_REPLICATION_GROUP_MESSAGE_ID = "replayReplicationGroupMessageId";
    public static final String REPLAY_STRATEGY_START_TIME = "replayStartTime";
    public static final String REPLAY_STRATEGY_TIMEZONE = "replayStartTimeTimezone";
    public static final String ACK_LAST_PROCESSED_MESSAGES = "ackLastProcessedMessages";
    public static final String ACK_LAST_PROCESSED_MESSAGES_DEFAULT = "false";
    public static final String SKIP_DUPLICATES = "skipDuplicates";
    public static final String SKIP_DUPLICATES_DEFAULT = "false";
    public static final String INCLUDE_HEADERS = "includeHeaders";
    public static final String INCLUDE_HEADERS_DEFAULT = "false";
    public static final String PARTITIONS = "partitions";
    public static final String PARTITIONS_DEFAULT = "1";
    public static final String OFFSET_INDICATOR = "offsetIndicator";
    public static final String OFFSET_INDICATOR_DEFAULT = "MESSAGE_ID";
    public static final String SOLACE_CONNECT_RETRIES = "connectRetries";
    public static final String SOLACE_RECONNECT_RETRIES = "reconnectRetries";
    public static final String SOLACE_CONNECT_RETRIES_PER_HOST = "connectRetriesPerHost";
    public static final String SOLACE_RECONNECT_RETRIES_WAIT_TIME = "reconnectRetryWaitInMillis";
    public static final String SOLACE_API_PROPERTIES_PREFIX = "solace.apiProperties.";
    public static final String OAUTH_CLIENT_ACCESSTOKEN_SOURCE = "solace.oauth.client.access-token.source";
    public static final String OAUTH_CLIENT_ACCESSTOKEN_SOURCE_DEFAULT = "file";
    public static final String OAUTH_CLIENT_ACCESSTOKEN = "solace.oauth.client.access-token";
    public static final String OAUTH_CLIENT_AUTHSERVER_URL = "solace.oauth.client.auth-server-url";
    public static final String OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE = "solace.oauth.client.auth-server.client-certificate.file";
    public static final String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_FILE = "solace.oauth.client.auth-server.truststore.file";
    public static final String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD = "solace.oauth.client.auth-server.truststore.password";
    public static final String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_TYPE = "solace.oauth.client.auth-server.truststore.type";
    public static final String OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_TYPE_DEFAULT = "JKS";
    public static final String OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE = "solace.oauth.client.auth-server.ssl.validate-certificate";
    public static final String OAUTH_CLIENT_AUTHSERVER_TLS_VERSION = "solace.oauth.client.auth-server.tls.version";
    public static final String OAUTH_CLIENT_AUTHSERVER_TLS_VERSION_DEFAULT = "TLSv1.2";
    public static final String OAUTH_CLIENT_CLIENT_ID = "solace.oauth.client.client-id";
    public static final String OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET = "solace.oauth.client.credentials.client-secret";
    public static final String OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL = "solace.oauth.client.token.refresh.interval";
    public static final String OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL_DEFAULT = "60";
    public static final String OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT = "solace.oauth.client.token.fetch.timeout";
    public static final String OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT_DEFAULT = "100";
}
