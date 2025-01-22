package com.solacecoe.connectors.spark.streaming.write;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceAbortMessage;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolacePublishStatus;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;

public class SolaceDataWriter implements DataWriter<InternalRow>, Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceDataWriter.class);
    private String topic;
    private String messageId;
    private final int batchSize;
    private final StructType schema;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;
    private final UnsafeProjection projection;
    private final Map<String, SolaceDataWriterCommitMessage> commitMessages;
    private final Map<String, SolaceAbortMessage> abortedMessages;
    private final List<UnsafeRow> buffer;
    private Exception exception;
    private final boolean includeHeaders;
    public SolaceDataWriter(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
        this.batchSize = Integer.parseInt(this.properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT));
        this.includeHeaders = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        this.topic = properties.getOrDefault(SolaceSparkStreamingProperties.TOPIC, null);
        this.messageId = properties.getOrDefault(SolaceSparkStreamingProperties.MESSAGE_ID, null);
        this.solaceBroker = new SolaceBroker(properties, "producer");
        this.solaceBroker.initProducer(getJCSMPStreamingPublishCorrelatingEventHandler());

        this.projection = createProjection();
        this.commitMessages = new HashMap<>();
        this.abortedMessages = new HashMap<>();
        this.buffer = new ArrayList<>();

//        System.out.println("Client " + this.solaceBroker.getUniqueName() + " Batch Size " + this.batchSize);
    }

    private void publishMessages() {
        for(UnsafeRow projectedRow : buffer) {
            if(this.topic == null) {
                this.topic = projectedRow.getUTF8String(3).toString();
            }

            if(this.messageId == null) {
                this.messageId = projectedRow.getUTF8String(0).toString();
            }
            byte[] payload;
            if(projectedRow.getBinary(1) != null) {
                payload = projectedRow.getBinary(1);
            } else {
                throw new RuntimeException("SolaceSparkConnector - Payload Column is not present in data frame.");
            }
            long timestamp = 0L;
            if(projectedRow.get(4, DataTypes.TimestampType) != null) {
                timestamp = Long.parseLong(projectedRow.get(4, DataTypes.TimestampType).toString());
            }
            UnsafeMapData headersMap = new UnsafeMapData();
            if(projectedRow.numFields() > 5 && projectedRow.getMap(5) != null) {
                headersMap = projectedRow.getMap(5);
            }
            String partitionKey = "";
            if(projectedRow.getUTF8String(2) != null) {
                partitionKey = projectedRow.getUTF8String(2).toString();
            }
//            if(!isBatch) {
//                XMLMessage xmlMessage = this.solaceBroker.createMessage(this.messageId,
//                        partitionKey, payload,
//                        timestamp, headersMap);
            this.solaceBroker.publishMessage(this.messageId, this.topic,
                    partitionKey, payload, timestamp, headersMap);
//            }
//            else if((this.batchMessages.size() + 1) == Integer.parseInt(this.properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT))) {
//                this.batchMessages.add(this.solaceBroker.createMultipleEntryMessage(this.messageId, this.topic,
//                        partitionKey, payload, timestamp, headersMap));
//                this.solaceBroker.publishBatch(this.batchMessages.toArray(new JCSMPSendMultipleEntry[batchMessages.size()]));
//                this.batchMessages.clear();
//            } else {
//                this.batchMessages.add(this.solaceBroker.createMultipleEntryMessage(this.messageId, this.topic,
//                        partitionKey, payload, timestamp, headersMap));
//            }
        }

        this.buffer.clear();
    }

    @Override
    public void write(InternalRow row) throws IOException {
        try {
            checkForException();
            UnsafeRow projectedRow = this.projection.apply(row);
            this.buffer.add(projectedRow);
//            if(this.buffer.size() >= batchSize) {
                publishMessages();
//            }
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
            throw new RuntimeException(exMessage);
        }
    }

    @Override
    public WriterCommitMessage commit() {
        checkForException();
        if(!this.buffer.isEmpty()) {
            publishMessages();
        }
        if(batchSize == 0 || (batchSize > 0 && this.commitMessages.size() < Integer.parseInt(this.properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT)))) {
            try {
                log.info("SolaceSparkConnector - Expected acknowledgements {}, Actual acknowledgements {}", this.properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT), this.commitMessages.size());
                log.info("SolaceSparkConnector - Sleeping for 3000ms to check for pending acknowledgments");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                log.error("SolaceSparkConnector - Interrupted while waiting for pending acknowledgments", e);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        checkForException();
        return new SolaceDataWriterCommitMessage(SolacePublishStatus.SUCCESS, "");
    }

    @Override
    public void abort() {
        log.error("SolaceSparkConnector - Publishing to Solace aborted", exception);
        Gson gson = new Gson();
        String exMessage = gson.toJson(abortedMessages, Map.class);
        abortedMessages.clear();
        buffer.clear();
        throw new RuntimeException(exMessage);
    }

    @Override
    public void close() {
        log.info("SolaceSparkConnector - SolaceDataWriter Closed");
        if(!this.buffer.isEmpty()) {
            publishMessages();
        }
        if(batchSize == 0 || (batchSize > 0 && this.commitMessages.size() < Integer.parseInt(this.properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT)))) {
            try {
                log.info("SolaceSparkConnector - Expected acknowledgements {}, Actual acknowledgements {}", this.properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT), this.commitMessages.size());
                log.info("SolaceSparkConnector - Sleeping for 3000ms to check for pending acknowledgments");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                log.error("SolaceSparkConnector - Interrupted while waiting for pending acknowledgments", e);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        commitMessages.clear();
        abortedMessages.clear();
        buffer.clear();
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
                log.info("SolaceSparkConnector - Message published successfully to Solace");
                SolaceDataWriterCommitMessage solaceWriterCommitMessage = new SolaceDataWriterCommitMessage(SolacePublishStatus.SUCCESS, "");
                commitMessages.put(o.toString(), solaceWriterCommitMessage);
            }

            @Override
            public void handleErrorEx(Object o, JCSMPException e, long l) {
                log.error("SolaceSparkConnector - Exception when publishing message to Solace", e);
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
            throw new RuntimeException(exMessage);
        }
    }
}
