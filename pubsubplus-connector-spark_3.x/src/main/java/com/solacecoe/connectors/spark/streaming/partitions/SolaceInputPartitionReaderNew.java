package com.solacecoe.connectors.spark.streaming.partitions;

import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.offset.SolaceSparkOffsetManager;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SolaceInputPartitionReaderNew implements PartitionReader<InternalRow>, Serializable {
    private final static Logger log = LogManager.getLogger(SolaceInputPartitionReaderNew.class);
    private final boolean includeHeaders;
    private final SolaceInputPartitionNew solaceInputPartition;
    private final Map<String, String> properties;
    private SolaceMessage solaceMessage;
    private SolaceBroker solaceBroker;
    private final int batchSize;
    private int messages = 0;
    private final String uniqueId;
    private final long receiveWaitTimeout;
    private final TaskContext taskContext;

    public SolaceInputPartitionReaderNew(SolaceInputPartitionNew inputPartition, boolean includeHeaders, Map<String, String> properties, TaskContext taskContext) {
        log.info("SolaceSparkConnector - Initializing Solace Input Partition reader with id {}", inputPartition.getId());
        this.includeHeaders = includeHeaders;
        this.solaceInputPartition = inputPartition;
        this.properties = properties;
        this.taskContext = taskContext;
        this.uniqueId = String.join(",", taskContext.getLocalProperty(StreamExecution.QUERY_ID_KEY()),
                taskContext.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY()),
                Integer.toString(taskContext.stageId()),
                Integer.toString(taskContext.partitionId()));
        this.batchSize = Integer.parseInt(properties.getOrDefault("batchSize", "0"));
        this.receiveWaitTimeout = Long.parseLong(properties.getOrDefault("receiveWaitTimeout", "10000"));
        boolean ackLastProcessedMessages = Boolean.parseBoolean(properties.getOrDefault("ackLastProcessedMessages", "false"));
        log.info("SolaceSparkConnector - Checking for connection {}", inputPartition.getId());
        if(SolaceConnectionManager.getConnection(inputPartition.getId()) != null) {
            solaceBroker = SolaceConnectionManager.getConnection(inputPartition.getId());
            if(solaceBroker != null) {
//                EventListener eventListener = new EventListener(inputPartition.getId());
//                if(ackLastProcessedMessages) {
//                    log.info("SolaceSparkConnector - last processed messages for list {}", SolaceSparkOffsetManager.getMessageIDs(uniqueId));
//                    eventListener = new EventListener(inputPartition.getId(), SolaceSparkOffsetManager.getMessageIDs(uniqueId), this.properties.getOrDefault("offsetIndicator", "MESSAGE_ID"));
//                }
//                // Initialize connection to Solace Broker
//                solaceBroker.addReceiver(eventListener);
                createReceiver(inputPartition.getId(), ackLastProcessedMessages);
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
            SolaceRecord solaceRecord = SolaceRecord.getMapper(this.properties.getOrDefault("offsetIndicator", "MESSAGE_ID")).map(solaceMessage.bytesXMLMessage);
            log.info("SolaceSparkConnector - Current message " + solaceRecord.getMessageId());
            long timestamp = solaceRecord.getSenderTimestamp();
            if (solaceRecord.getSenderTimestamp() == 0) {
                timestamp = System.currentTimeMillis();
            }
            InternalRow row;
            if(this.includeHeaders) {
                log.info("SolaceSparkConnector - Adding event headers to Spark row");
                Map<String, Object> userProperties = (solaceRecord.getProperties() != null) ? solaceRecord.getProperties() : new HashMap<>();
                if(solaceRecord.getSequenceNumber() != null) {
                    userProperties.put("solace_sequence_number", solaceRecord.getSequenceNumber());
                }
                userProperties.put("solace_expiration", solaceRecord.getExpiration());
                userProperties.put("solace_time_to_live", solaceRecord.getTimeToLive());
                userProperties.put("solace_priority", solaceRecord.getPriority());
                MapData mapData = new ArrayBasedMapData(new GenericArrayData(userProperties.keySet().stream().map(key -> UTF8String.fromString(key)).toArray()), new GenericArrayData(userProperties.values().stream().map(value -> value.toString().getBytes(StandardCharsets.UTF_8)).toArray()));
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

            SolaceSparkOffsetManager.addMessageID(this.uniqueId, solaceRecord.getMessageId());
            SolaceSparkOffsetManager.addMessage(uniqueId, solaceMessage);
            log.info("SolaceSparkConnector - Updated offset manager with offset");
            log.info("SolaceSparkConnector - Created Spark row for message with ID {}", solaceRecord.getMessageId());
            return row;
        } catch (Exception e) {
            log.error("SolaceSparkConnector- Exception while reading message", e);
            throw new RuntimeException(e);
        }
    }

    private SolaceMessage getNextMessage() {
        LinkedBlockingQueue<SolaceMessage> queue = solaceBroker.getMessages(0);
        while(batchSize == 0 || (batchSize > 0 && messages < batchSize)) {
            try {
                solaceMessage = queue.poll(this.receiveWaitTimeout, TimeUnit.MILLISECONDS);
                if (solaceMessage == null) {
                    return null;
                } else {
                    if(SolaceSparkOffsetManager.containsMessageID(solaceMessage.bytesXMLMessage.getReplicationGroupMessageId().toString())) {
                        log.info("SolaceSparkConnector - Message is present in previous partition. Skip to next message");
                    } else {
                        if(batchSize > 0) {
                            messages++;
                        }
                        return solaceMessage;
                    }
                }
            } catch (InterruptedException e) {
                return null;
            }
        }

        return null;
    }

    @Override
    public void close() {
        log.info("SolaceSparkConnector - Input partition reader with ID {} is closed", this.solaceInputPartition.getId());
    }

    private void registerTaskListener() {
        this.taskContext.addTaskCompletionListener(context -> {
            log.info("SolaceSparkConnector - Task {} state is completed :: {}, failed :: {}, interrupted :: {}", uniqueId, context.isCompleted(), context.isFailed(), context.isInterrupted());
            if(context.isInterrupted()) {
                log.info("SolaceSparkConnector - Closing connections to Solace as task is interrupted");
                SolaceConnectionManager.close();
            } else {
                if (context.isCompleted()) {
                    SolaceSparkOffsetManager.ackMessages(uniqueId);
                    solaceBroker.closeReceivers();
                    String processedMessageIDs = SolaceSparkOffsetManager.getProcessedMessagesIDs(uniqueId);
                    if (processedMessageIDs != null) {
                        solaceBroker.initProducer();
                        solaceBroker.publishMessage("solace/spark/connector/offset", processedMessageIDs);
                        solaceBroker.closeProducer();
                        SolaceSparkOffsetManager.removeProcessedMessagesIDs(uniqueId);
                    }
                }
            }
        });
    }

    private void createNewConnection(int inputPartitionId, boolean ackLastProcessedMessages) {
        log.info("SolaceSparkConnector - Solace Connection Details Host : {}, VPN : {}, Username : {}", properties.get("host"), properties.get("vpn"), properties.get("username"));
        solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"));
        SolaceConnectionManager.addConnection(inputPartitionId, solaceBroker);
        createReceiver(inputPartitionId, ackLastProcessedMessages);
    }

    private void createReceiver(int inputPartitionId, boolean ackLastProcessedMessages) {
        EventListener eventListener = new EventListener(inputPartitionId);
        if(ackLastProcessedMessages) {
            log.info("SolaceSparkConnector - last processed messages for list {}", SolaceSparkOffsetManager.getMessageIDs(uniqueId));
            eventListener = new EventListener(inputPartitionId, SolaceSparkOffsetManager.getMessageIDs(uniqueId), this.properties.getOrDefault("offsetIndicator", "MESSAGE_ID"));
        }
        // Initialize connection to Solace Broker
        solaceBroker.addReceiver(eventListener);
    }
}
