package com.solacecoe.connectors.spark.streaming.properties;

public class SolaceSparkStreamingProperties {
    public static String HOST = "host";
    public static String VPN = "vpn";
    public static String USERNAME = "username";
    public static String PASSWORD = "password";
    public static String QUEUE = "queue";
    public static String QUEUE_RECEIVE_WAIT_TIMEOUT = "queue.receiveWaitTimeout";
    public static String QUEUE_RECEIVE_WAIT_TIMEOUT_DEFAULT = "10000";
    public static String BATCH_SIZE = "batchSize";
    public static String BATCH_SIZE_DEFAULT = "0";
    public static String ACK_LAST_PROCESSED_MESSAGES = "ackLastProcessedMessages";
    public static String ACK_LAST_PROCESSED_MESSAGES_DEFAULT = "false";
    public static String INCLUDE_HEADERS = "includeHeaders";
    public static String INCLUDE_HEADERS_DEFAULT = "false";
    public static String PARTITIONS = "partitions";
    public static String PARTITIONS_DEFAULT = "1";
    public static String OFFSET_INDICATOR = "offsetIndicator";
    public static String OFFSET_INDICATOR_DEFAULT = "MESSAGE_ID";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_NAME = "lvq.name";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_TOPIC = "lvq.topic";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_NAME = "solace.spark.connector.state";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC = "solace/spark/connector/offset";
}
