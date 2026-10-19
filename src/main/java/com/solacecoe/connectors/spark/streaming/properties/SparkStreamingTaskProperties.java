package com.solacecoe.connectors.spark.streaming.properties;

public final class SparkStreamingTaskProperties {

    /** Mirrors {@code org.apache.spark.sql.execution.streaming.StreamExecution.QUERY_ID_KEY}. */
    public static final String QUERY_ID_KEY = "sql.streaming.queryId";

    /** Mirrors {@code org.apache.spark.sql.execution.streaming.MicroBatchExecution.BATCH_ID_KEY}. */
    public static final String BATCH_ID_KEY = "streaming.sql.batchId";

    private SparkStreamingTaskProperties() {
        // constants holder
    }
}
