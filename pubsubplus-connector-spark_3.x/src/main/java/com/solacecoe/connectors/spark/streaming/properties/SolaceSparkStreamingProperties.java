package com.solacecoe.connectors.spark.streaming.properties;

public class SolaceSparkStreamingProperties {
    public static String HOST = "host";
    public static String VPN = "vpn";
    public static String USERNAME = "USERNAME";
    public static String PASSWORD = "PASSWORD";
    public static String QUEUE = "queue";
    public static String BATCH_SIZE = "batchSize";
    public static String ACK_LAST_PROCESSED_MESSAGES = "ackLastProcessedMessages";
    public static String INCLUDE_HEADERS = "includeHeaders";
    public static String PARTITIONS = "partitions";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_NAME = "solace.spark.connector.state";
    public static String SOLACE_SPARK_CONNECTOR_LVQ_TOPIC = "solace/spark/connector/offset";
}
