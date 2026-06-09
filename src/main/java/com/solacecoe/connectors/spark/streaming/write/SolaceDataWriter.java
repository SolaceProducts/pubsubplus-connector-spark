package com.solacecoe.connectors.spark.streaming.write;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolacePublishAbortException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolacePublishAckInterruptedException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolacePublishAckTimeoutException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolacePublishException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceAbortMessage;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolacePublishStatus;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.*;

public class SolaceDataWriter implements DataWriter<InternalRow> {
    private static final Logger log = LoggerFactory.getLogger(SolaceDataWriter.class);
    private String topic;
    private String messageId;
    private final StructType schema;
    private final Map<String, String> properties;
    private SolaceBroker solaceBroker;
    private final transient UnsafeProjection projection;
    private final ConcurrentHashMap<String, SolaceDataWriterCommitMessage> commitMessages;
    private final ConcurrentHashMap<String, SolaceAbortMessage> abortedMessages;
    private Exception exception;
    private final boolean includeHeaders;
    private final boolean hasDefaultTopic;
//    private final boolean hasDefaultMessageId;
    private int publishedMessages = 0;
    private CompletableFuture<Void> allAcksReceived = new CompletableFuture<>();
    public SolaceDataWriter(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
        this.includeHeaders = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        this.topic = properties.getOrDefault(SolaceSparkStreamingProperties.TOPIC, null);
//        this.messageId = properties.getOrDefault(SolaceSparkStreamingProperties.MESSAGE_ID, null);
        hasDefaultTopic = this.topic != null;
//        hasDefaultMessageId = this.messageId != null;
        try {
            this.solaceBroker = new SolaceBroker(properties, "producer");
            this.solaceBroker.initProducer(getJCSMPStreamingPublishCorrelatingEventHandler());
        } catch (Exception e) {
            if(this.solaceBroker != null) {
                this.solaceBroker.close();
            }
            throw new SolacePublishException(e.getCause());
        }

        this.projection = createProjection();
        this.commitMessages = new ConcurrentHashMap<>();
        this.abortedMessages = new ConcurrentHashMap<>();
    }

    private void publishMessages(UnsafeRow projectedRow) {
        if(!hasDefaultTopic) {
            this.topic = projectedRow.getUTF8String(3).toString();
        }

//        if(!hasDefaultMessageId) {
//            this.messageId = projectedRow.getUTF8String(0).toString();
//        }
        this.messageId = projectedRow.getUTF8String(0).toString();
        byte[] payload;
        if(projectedRow.getBinary(1) != null) {
            payload = projectedRow.getBinary(1);
        } else {
            throw new SolacePublishException("SolaceSparkConnector - Payload Column is not present in data frame.");
        }
        long timestamp = 0L;
        if(projectedRow.get(4, DataTypes.TimestampType) != null) {
            // TimestampType always returns long. So safe to use getLong
            timestamp = projectedRow.getLong(4);
        }
        UnsafeMapData headersMap = new UnsafeMapData();
        if(projectedRow.numFields() > 5 && projectedRow.getMap(5) != null) {
            headersMap = projectedRow.getMap(5);
        }
        String partitionKey = "";
        if(projectedRow.getUTF8String(2) != null) {
            partitionKey = projectedRow.getUTF8String(2).toString();
        }
        try {
            this.solaceBroker.publishMessage(this.messageId, this.topic,
                    partitionKey, payload, timestamp, headersMap);
            publishedMessages++;
        } catch (Exception e) {
            this.solaceBroker.close();
            throw new SolacePublishException(e.getCause());
        }
    }

    @Override
    public void write(InternalRow row) throws IOException {
        try {
            checkForException();
            UnsafeRow projectedRow = this.projection.apply(row);
            publishMessages(projectedRow);
            checkForException();
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString();
            SolaceAbortMessage abortMessage = new SolaceAbortMessage(SolacePublishStatus.FAILED, sStackTrace);
            abortedMessages.put(this.messageId != null ? this.messageId : row.getUTF8String(0).toString(), abortMessage);
            exception = e;
            Gson gson = new Gson();
            String exMessage = gson.toJson(abortedMessages, Map.class);
            abortedMessages.clear();
            throw new SolacePublishException(exMessage);
        }
    }

    @Override
    public WriterCommitMessage commit() {
        checkForException();
        long ackTimeout = Long.parseLong(this.properties.getOrDefault(SolaceSparkStreamingProperties.PUBLISH_ACK_TIMEOUT, SolaceSparkStreamingProperties.PUBLISH_ACK_TIMEOUT_DEFAULT));
        boolean failOnTimeout = Boolean.parseBoolean(this.properties.getOrDefault(
                SolaceSparkStreamingProperties.PUBLISH_ACK_TIMEOUT_FAIL_ON_ERROR,
                SolaceSparkStreamingProperties.PUBLISH_ACK_TIMEOUT_FAIL_ON_ERROR_DEFAULT));
        try {
            log.info("SolaceSparkConnector - Waiting for acknowledgements. " +
                            "Expected: {}, Received: {}",
                    publishedMessages, this.commitMessages.size());

            // Block until all acks received or timeout — no polling, no loop
            allAcksReceived.get(ackTimeout, TimeUnit.MILLISECONDS);

            log.info("SolaceSparkConnector - All acknowledgements received. " +
                            "Expected: {}, Received: {}",
                    publishedMessages, this.commitMessages.size());

        } catch (TimeoutException e) {
            if (failOnTimeout) {
                // Fail the batch — throws exception and stops processing
                throw new SolacePublishAckTimeoutException(
                        String.format("SolaceSparkConnector - Timed out after %dms waiting for " +
                                        "acknowledgements. Expected: %d, Received: %d",
                                ackTimeout, publishedMessages, this.commitMessages.size()), e);
            } else {
                // Log and continue — does not stop processing
                log.warn("SolaceSparkConnector - Timed out after {}ms waiting for acknowledgements. " +
                                "Expected: {}, Received: {}. Continuing with next batch.",
                        ackTimeout, publishedMessages, this.commitMessages.size());
            }
        } catch (ExecutionException e) {
            throw new SolacePublishAckInterruptedException(
                    "SolaceSparkConnector - Error while waiting for acknowledgements", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolacePublishAckInterruptedException(
                    "SolaceSparkConnector - Interrupted while waiting for acknowledgements", e);
        }
        checkForException();
        return new SolaceDataWriterCommitMessage(SolacePublishStatus.SUCCESS, "");
    }

    @Override
    public void abort() {
        log.error("SolaceSparkConnector - Publishing to Solace aborted", exception);
        Gson gson = new Gson();
        String exMessage = gson.toJson(abortedMessages, Map.class);
        throw new SolacePublishAbortException(exMessage);
    }

    @Override
    public void close() {
        log.info("SolaceSparkConnector - SolaceDataWriter Closed");
        commitMessages.clear();
        abortedMessages.clear();
        this.solaceBroker.close();
    }

    private UnsafeProjection createProjection() {
        List<Attribute> attributeList = new ArrayList<>();
        this.schema.foreach(field -> attributeList.add(DataTypeUtils.toAttribute(field)));
        Seq<Attribute> attributes = JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq();

        return UnsafeProjection.create(JavaConverters.asScalaIteratorConverter(Arrays.stream(getExpressions(attributes)).iterator()).asScala().toSeq(),
                JavaConverters.asScalaIteratorConverter(attributeList.iterator()).asScala().toSeq()
        );
    }

    private Expression[] getExpressions(Seq<Attribute> attributes) {

        Expression headerExpression = new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.headers().name(), SolaceSparkSchemaProperties.headers().dataType(), null, false).getExpression();
        if(!this.includeHeaders) {
            return new Expression[] {
                    // DataTypeUtils.toAttribute(new StructField("Id", DataTypes.StringType, true, Metadata.empty()))
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.id().name(), SolaceSparkSchemaProperties.id().dataType(), null, (this.messageId == null)).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.payload().name(), SolaceSparkSchemaProperties.payload().dataType(), null, false).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.partitionKey().name(), SolaceSparkSchemaProperties.partitionKey().dataType(), null, false).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.topic().name(), SolaceSparkSchemaProperties.topic().dataType(), null, (this.topic == null)).getExpression(),
                    new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.timestamp().name(), SolaceSparkSchemaProperties.timestamp().dataType(), null, false).getExpression(),
            };
        }
        return new Expression[] {
                // DataTypeUtils.toAttribute(new StructField("Id", DataTypes.StringType, true, Metadata.empty()))
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.id().name(), SolaceSparkSchemaProperties.id().dataType(), null, (this.messageId == null)).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.payload().name(), SolaceSparkSchemaProperties.payload().dataType(), null, false).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.partitionKey().name(), SolaceSparkSchemaProperties.partitionKey().dataType(), null, false).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.topic().name(), SolaceSparkSchemaProperties.topic().dataType(), null, (this.topic == null)).getExpression(),
                new SolaceRowExpression(attributes, SolaceSparkSchemaProperties.timestamp().name(), SolaceSparkSchemaProperties.timestamp().dataType(), null, false).getExpression(),
                headerExpression
        };
    }

    private JCSMPStreamingPublishCorrelatingEventHandler getJCSMPStreamingPublishCorrelatingEventHandler() {
        return new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object o) {
                log.info("SolaceSparkConnector - Message published successfully to Solace on topic {}", topic);
                SolaceDataWriterCommitMessage solaceWriterCommitMessage = new SolaceDataWriterCommitMessage(SolacePublishStatus.SUCCESS, "");
                commitMessages.put(o.toString(), solaceWriterCommitMessage);

                // Complete the future when all expected acks have arrived
                if (commitMessages.size() >= publishedMessages) {
                    log.info("SolaceSparkConnector - All {} acknowledgements received", publishedMessages);
                    allAcksReceived.complete(null);
                }
            }

            @Override
            public void handleErrorEx(Object o, JCSMPException e, long l) {
                log.error("SolaceSparkConnector - Exception when publishing message to Solace on topic {}", topic, e);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                String sStackTrace = sw.toString();
                SolaceAbortMessage abortMessage = new SolaceAbortMessage(SolacePublishStatus.FAILED, sStackTrace);
                abortedMessages.put(o.toString(), abortMessage);
                exception = e;
            }
        };
    }

    private void checkForException() {
        if(exception != null) {
            Gson gson = new Gson();
            String exMessage = gson.toJson(abortedMessages, Map.class);
            abortedMessages.clear();
            throw new SolacePublishException(exMessage);
        }
    }
}
