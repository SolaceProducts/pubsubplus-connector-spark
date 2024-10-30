package com.solacecoe.connectors.spark.streaming.properties;

public final class SolaceSparkHeaders {
    /**
     * The prefix used for all headers in this class.
     */
    static final String PREFIX = SolaceHeaders.PREFIX + "scst_";

    /**
     * <p><b>Acceptable Value Type:</b> {@link String}</p>
     * <p><b>Access:</b> Write</p>
     * <br>
     * <p>The partition key for PubSub+ partitioned queues.</p>
     */
    public static final String PARTITION_KEY = PREFIX + "partitionKey";

    /**
     * <p><b>Acceptable Value Type:</b> {@link Integer}</p>
     * <p><b>Access:</b> Read</p>
     * <p><b>Default Value: </b>{@code 1}</p>
     * <br>
     * <p>A static number set by the publisher to indicate the Spring Cloud Stream Solace message version.</p>
     */
    public static final String MESSAGE_VERSION = PREFIX + "messageVersion";

    /**
     * <p><b>Acceptable Value Type:</b> {@link Boolean}</p>
     * <p><b>Access:</b> Internal Binder Use Only</p>
     * <br>
     * <p>Is {@code true} if a Solace Spring Cloud Stream binder has serialized the payload before publishing
     * it to a broker. Is undefined otherwise.</p>
     */
    public static final String SERIALIZED_PAYLOAD = PREFIX + "serializedPayload";

    /**
     * <p><b>Acceptable Value Type:</b> {@link String}</p>
     * <p><b>Access:</b> Internal Binder Use Only</p>
     * <br>
     * <p>A JSON String array of header names where each entry indicates that that header’s value was serialized by a
     * Solace Spring Cloud Stream binder before publishing it to a broker.</p>
     */
    public static final String SERIALIZED_HEADERS = PREFIX + "serializedHeaders";

    /**
     * <p><b>Acceptable Value Type:</b> {@link String}</p>
     * <p><b>Access:</b> Internal Binder Use Only</p>
     * <p><b>Default Value: </b>{@code "base64"}</p>
     * <br>
     * <p>The encoding algorithm used to encode the headers indicated by {@link #SERIALIZED_HEADERS}.</p>
     */
    public static final String SERIALIZED_HEADERS_ENCODING = PREFIX + "serializedHeadersEncoding";


    public static final String CONFIRM_CORRELATION = PREFIX + "confirmCorrelation";

    /**
     * <p><b>Acceptable Value Type:</b> {@link Boolean}</p>
     * <p><b>Access:</b> Read</p>
     * <br>
     * <p>Present and true to indicate when the PubSub+ message payload was null.</p>
     */
    public static final String NULL_PAYLOAD = PREFIX + "nullPayload";

    /**
     * <p><b>Acceptable Value Type:</b> {@code List<Map<String, Object>>}</p>
     * <p><b>Access:</b> Read</p>
     * <br>
     * <p>Only applicable when {@code batchMode} is {@code true}. The consolidated list of message headers for a
     * batch of messages where the headers for each payload element is in this list’s corresponding index.</p>
     */
    public static final String BATCHED_HEADERS = PREFIX + "batchedHeaders";

    /**
     * <p><b>Acceptable Value Type:</b> String</p>
     * <p><b>Access:</b> Write</p>
     * <br>
     * <p> Only applicable when {@code scst_targetDestination} is set.</p>
     * <ul>
     *   <li><b>topic</b>: Specifies that the dynamic destination is a topic</li>
     *   <li><b>queue</b>: Specifies that the dynamic destination is a queue</li>
     * </ul>
     * <p>When absent, the binding’s configured destination-type is used.</p>
     */
    public static final String TARGET_DESTINATION_TYPE = PREFIX + "targetDestinationType";
}
