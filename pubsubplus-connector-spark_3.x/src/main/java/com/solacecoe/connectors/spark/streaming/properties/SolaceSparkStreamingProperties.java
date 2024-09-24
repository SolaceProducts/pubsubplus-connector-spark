package com.solacecoe.connectors.spark.streaming.properties;

public class SolaceSparkStreamingProperties {
    public static String HOST = "host";
    public static String VPN = "vpn";
    public static String USERNAME = "username";
    public static String PASSWORD = "password";
    public static String QUEUE = "queue";
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
    public static String SOLACE_CONNECT_RETRIES = "connectRetries";
    public static String SOLACE_RECONNECT_RETRIES = "reconnectRetries";
    public static String SOLACE_CONNECT_RETRIES_PER_HOST = "connectRetriesPerHost";
    public static String SOLACE_RECONNECT_RETRIES_WAIT_TIME = "reconnectRetryWaitInMillis";
    public static String SOLACE_API_PROPERTIES_PREFIX = "solace.apiProperties.";
    public static String OAUTH_CLIENT_AUTHSERVER_URL = "oauth.client.auth-server-url";
    public static String OAUTH_CLIENT_CLIENT_ID = "oauth.client.client-id";
    public static String OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET = "oauth.client.credentials.client-secret";
    public static String OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL = "oauth.client.token.refresh.interval";
    public static String OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL_DEFAULT = "60";
    public static String OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT = "oauth.client.token.fetch.timeout";
    public static String OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT_DEFAULT = "10";
}
