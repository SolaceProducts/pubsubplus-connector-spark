package com.solacecoe.connectors.spark.streaming.partitions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.streaming.properties.SolaceHeaders;
import com.solacecoe.connectors.spark.streaming.solace.*;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.offset.SolaceMessageTracker;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceMessageException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceSessionException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.SDTException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.streaming.MicroBatchExecution;
import org.apache.spark.sql.execution.streaming.StreamExecution;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class SolaceInputPartitionReader implements PartitionReader<InternalRow>, Serializable {
    private final transient Logger log = LogManager.getLogger(SolaceInputPartitionReader.class);
    private final boolean includeHeaders;
    private final SolaceInputPartition solaceInputPartition;
    private final Map<String, String> properties;
    private SolaceMessage solaceMessage;
    private SolaceBroker solaceBroker;
    private final int batchSize;
    private int messages = 0;
    private final String uniqueId;
    private final long receiveWaitTimeout;
    private final TaskContext taskContext;
    private final String lastKnownOffset;
    private final boolean closeReceiversOnPartitionClose;

    public SolaceInputPartitionReader(SolaceInputPartition inputPartition, boolean includeHeaders, String lastKnownOffset, Map<String, String> properties, TaskContext taskContext) {
        log.info("SolaceSparkConnector - Initializing Solace Input Partition reader with id {}", inputPartition.getId());
        this.solaceInputPartition = inputPartition;
        this.includeHeaders = includeHeaders;
        this.lastKnownOffset = lastKnownOffset;
        this.properties = properties;
        this.taskContext = taskContext;
        this.uniqueId = String.join(",", taskContext.getLocalProperty(StreamExecution.QUERY_ID_KEY()),
                taskContext.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY()),
                Integer.toString(taskContext.stageId()),
                Integer.toString(taskContext.partitionId()));
        this.batchSize = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT));
        this.receiveWaitTimeout = Long.parseLong(properties.getOrDefault(SolaceSparkStreamingProperties.QUEUE_RECEIVE_WAIT_TIMEOUT, SolaceSparkStreamingProperties.QUEUE_RECEIVE_WAIT_TIMEOUT_DEFAULT));
        this.closeReceiversOnPartitionClose = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.CLOSE_RECEIVERS_ON_PARTITION_CLOSE, SolaceSparkStreamingProperties.CLOSE_RECEIVERS_ON_PARTITION_CLOSE_DEFAULT));
        boolean ackLastProcessedMessages = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES, SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES_DEFAULT));
        log.info("SolaceSparkConnector - Checking for connection {}", inputPartition.getId());

        if (SolaceConnectionManager.getConnection(inputPartition.getId()) != null) {
            solaceBroker = SolaceConnectionManager.getConnection(inputPartition.getId());
            if (solaceBroker != null) {
                if (closeReceiversOnPartitionClose) {
                    createReceiver(inputPartition.getId(), ackLastProcessedMessages);
                }
            } else {
                log.warn("SolaceSparkConnector - Existing Solace connection not available for partition {}. Creating new connection", inputPartition.getId());
                createNewConnection(inputPartition.getId(), ackLastProcessedMessages);
            }
        } else {
            createNewConnection(inputPartition.getId(), ackLastProcessedMessages);
        }

        log.info("SolaceSparkConnector - Acquired connection to Solace broker for partition {}", inputPartition.getId());
        registerTaskListener();
    }

    @Override
    public boolean next() {
        solaceMessage = getNextMessage();
        return solaceMessage != null;
    }

    @Override
    public InternalRow get() {
        try {
            SolaceRecord solaceRecord = SolaceRecord.getMapper(this.properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT)).map(solaceMessage.bytesXMLMessage);
            log.info("SolaceSparkConnector - Current message {}", solaceRecord.getMessageId());
            long timestamp = solaceRecord.getSenderTimestamp();
            if (solaceRecord.getSenderTimestamp() == 0) {
                timestamp = System.currentTimeMillis();
            }
            InternalRow row;
            if(this.includeHeaders) {
                log.info("SolaceSparkConnector - Adding event headers to Spark row");
                Map<String, Object> headers = getStringObjectMap(solaceRecord);
                MapData mapData = new ArrayBasedMapData(new GenericArrayData(headers.keySet().stream().filter(key -> headers.get(key) != null).map(UTF8String::fromString).toArray()), new GenericArrayData(headers.values().stream().filter(Objects::nonNull).map(value -> value.toString().getBytes(StandardCharsets.UTF_8)).toArray()));
                row = new GenericInternalRow(new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
                        solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
                        DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp)),mapData
                });
            } else {
                row = new GenericInternalRow(new Object[]{UTF8String.fromString(solaceRecord.getMessageId()),
                        solaceRecord.getPayload(), UTF8String.fromString(solaceRecord.getPartitionKey()), UTF8String.fromString(solaceRecord.getDestination()),
                        DateTimeUtils.fromJavaTimestamp(new Timestamp(timestamp))
                });
            }

            SolaceMessageTracker.addMessageID(this.uniqueId, solaceRecord.getMessageId());
            SolaceMessageTracker.addMessage(uniqueId, solaceMessage);
            log.info("SolaceSparkConnector - Updated offset manager with offset");
            log.info("SolaceSparkConnector - Created Spark row for message with ID {}", solaceRecord.getMessageId());
            return row;
        } catch (Exception e) {
            log.error("SolaceSparkConnector- Exception while reading message", e);
            throw new SolaceMessageException(e);
        }
    }

    private Map<String, Object> getStringObjectMap(SolaceRecord solaceRecord) {
        Map<String, Object> headers = new HashMap<>();
        Map<String, Object> userProperties = (solaceRecord.getProperties() != null) ? solaceRecord.getProperties() : new HashMap<>();
        if(!userProperties.isEmpty()) {
            headers.putAll(userProperties);
        }
        if(solaceRecord.getSequenceNumber() != null) {
            headers.put(SolaceHeaders.SEQUENCE_NUMBER, solaceRecord.getSequenceNumber());
        }
        headers.put(SolaceHeaders.EXPIRATION, solaceRecord.getExpiration());
        headers.put(SolaceHeaders.TIME_TO_LIVE, solaceRecord.getTimeToLive());
        headers.put(SolaceHeaders.PRIORITY, solaceRecord.getPriority());
        headers.put(SolaceHeaders.REDELIVERED, solaceRecord.isRedelivered());
        return headers;
    }

    private SolaceMessage getNextMessage() {
        LinkedBlockingQueue<SolaceMessage> queue = solaceBroker.getMessages(0);
        if(queue != null) {
            while(shouldProcessMoreMessages(batchSize, messages)) {
                try {
                    solaceMessage = queue.poll(receiveWaitTimeout, TimeUnit.MILLISECONDS);
                    if (solaceMessage == null) {
                        return null;
                    }

                    if(batchSize > 0) {
                        messages++;
                    }
                    if(isMessageAlreadyProcessed(solaceMessage)) {
                        log.info("Message is added to previous partitions for processing. Moving to next message");
                    } else {
                        return solaceMessage;
                    }

                } catch (InterruptedException | SDTException e) {
                    log.warn("No messages available within specified receiveWaitTimeout", e);
                    return null;
                }
            }
        }


        return null;
    }

    private boolean isMessageAlreadyProcessed(SolaceMessage solaceMessage) throws SDTException {
        String messageId = SolaceUtils.getMessageID(
                solaceMessage.bytesXMLMessage,
                this.properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT)
        );
        return SolaceMessageTracker.containsMessageID(messageId);
    }

    private boolean shouldProcessMoreMessages(int batchSize, int messages) {
        return batchSize == 0 || (batchSize > 0 && messages < batchSize);
    }

    @Override
    public void close() {
        log.info("SolaceSparkConnector - Input partition reader with ID {} with task {} is closed", this.solaceInputPartition.getId(), this.uniqueId);
    }

    private void registerTaskListener() {
        this.taskContext.addTaskCompletionListener(context -> {
            log.info("SolaceSparkConnector - Task {} state is completed :: {}, failed :: {}, interrupted :: {}", uniqueId, context.isCompleted(), context.isFailed(), context.isInterrupted());
            if(context.isInterrupted() || context.isFailed()) {
                log.info("SolaceSparkConnector - Closing connections to Solace as task {} is interrupted or failed", String.join(",", context.getLocalProperty(StreamExecution.QUERY_ID_KEY()),
                        context.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY()),
                        Integer.toString(context.stageId()),
                        Integer.toString(context.partitionId())));
                SolaceConnectionManager.close();
            } else if (context.isCompleted()) {
                log.info("SolaceSparkConnector - Total time taken by executor is {}ms for Task {}", context.taskMetrics().executorRunTime(),uniqueId);
                SolaceMessageTracker.ackMessages(uniqueId);
                String processedMessageIDs = SolaceMessageTracker.getProcessedMessagesIDs(uniqueId);
                if (processedMessageIDs != null) {
                    SolaceSparkPartitionCheckpoint solaceSparkPartitionCheckpoint = SolaceMessageTracker.getCheckpoint(this.solaceInputPartition.getId());
                    if(solaceSparkPartitionCheckpoint != null) {
                        solaceSparkPartitionCheckpoint.setStartOffset(solaceSparkPartitionCheckpoint.getEndOffset());
                        solaceSparkPartitionCheckpoint.setEndOffset(solaceSparkPartitionCheckpoint.getEndOffset() + batchSize);
                        solaceSparkPartitionCheckpoint.setMessageIDs(processedMessageIDs);
                        solaceSparkPartitionCheckpoint.setPartitionId(this.solaceInputPartition.getId());
                    } else {
                        solaceSparkPartitionCheckpoint = new SolaceSparkPartitionCheckpoint(0, batchSize, processedMessageIDs, this.solaceInputPartition.getId());
                    }
                    SolaceMessageTracker.updateCheckpoint(solaceSparkPartitionCheckpoint);
                    solaceBroker.publishMessage(properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC), SolaceMessageTracker.getCheckpoint());
                    SolaceMessageTracker.removeProcessedMessagesIDs(uniqueId);
                }
                if(closeReceiversOnPartitionClose) {
                    solaceBroker.closeReceivers();
                }
            }
        });
    }

    private void createNewConnection(String inputPartitionId, boolean ackLastProcessedMessages) throws SolaceSessionException {
        log.info("SolaceSparkConnector - Solace Connection Details Host : {}, VPN : {}, Username : {}", properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME));
        solaceBroker = new SolaceBroker(properties, "consumer");
        solaceBroker.initProducer();
        SolaceConnectionManager.addConnection(inputPartitionId, solaceBroker);
        createReceiver(inputPartitionId, ackLastProcessedMessages);
    }

    private void createReceiver(String inputPartitionId, boolean ackLastProcessedMessages) throws SolaceSessionException {
        EventListener eventListener = new EventListener(inputPartitionId);
        if(ackLastProcessedMessages) {
            log.info("SolaceSparkConnector - last processed messages for list {}", SolaceMessageTracker.getMessageIDs(uniqueId));
            JsonObject toJsonObject = new Gson().fromJson(this.lastKnownOffset, JsonObject.class);
            String lastKnownMessageIDs = toJsonObject.has("messageIDs") ? toJsonObject.get("messageIDs").getAsString() : "";
            List<String> messageIDs = Arrays.stream(lastKnownMessageIDs.split(",")).collect(Collectors.toList());
            eventListener = new EventListener(inputPartitionId, messageIDs, this.properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT));
        }
        // Initialize connection to Solace Broker
        solaceBroker.addReceiver(eventListener);
    }
}
