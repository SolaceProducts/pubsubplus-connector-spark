package com.solacecoe.connectors.spark.streaming.partitions;

import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.offset.SolaceSparkOffsetManager;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.read.PartitionReader;
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
//    private final SolaceBroker solaceBroker;
//    private final SolaceConnectionManager solaceConnectionManager;
    private SolaceMessage solaceMessage;
    private final SolaceBroker solaceBroker;

    public SolaceInputPartitionReaderNew(SolaceInputPartitionNew inputPartition, boolean includeHeaders, Map<String, String> properties) {
        log.info("SolaceSparkConnector - Initializing Solace Input Partition reader with id {}", inputPartition.getId());
        this.includeHeaders = includeHeaders;
        this.solaceInputPartition = inputPartition;
        this.properties = properties;

        log.info("SolaceSparkConnector - Checking for connection {}", inputPartition.getId());
        if(SolaceConnectionManager.getConnection(inputPartition.getId()) != null) {
            solaceBroker = SolaceConnectionManager.getConnection(inputPartition.getId());
            solaceBroker.closeReceivers();
            EventListener eventListener = new EventListener(inputPartition.getId());
            // Initialize connection to Solace Broker
            solaceBroker.addReceiver(eventListener);
            log.info("SolaceSparkConnector - Acquired connection to Solace broker for partition {}", inputPartition.getId());
        } else {
            log.info("SolaceSparkConnector - Solace Connection Details Host : {}, VPN : {}, Username : {}", properties.get("host"), properties.get("vpn"), properties.get("username"));
            solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"));
            SolaceConnectionManager.addConnection(solaceBroker);
            EventListener eventListener = new EventListener(inputPartition.getId());
            // Initialize connection to Solace Broker
            solaceBroker.addReceiver(eventListener);
            log.info("SolaceSparkConnector - Acquired connection to Solace broker for partition {}", inputPartition.getId());
        }
    }

    @Override
    public boolean next() {
        solaceMessage = getNextMessage();
        return solaceMessage != null;
//        return solaceBroker.getMessages(this.solaceInputPartition.getId()) != null && !solaceBroker.getMessages(this.solaceInputPartition.getId()).isEmpty();
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

            SolaceSparkOffsetManager.addMessage(solaceRecord.getMessageId(), solaceMessage);
            log.info("SolaceSparkConnector - Updated message tracker for offset");
//            if(SolaceSparkOffsetManager.messageTrackers.getMessages(this.solaceInputPartition.getId())) {
//                SolaceMessageTracker solaceMessageTracker = SolaceSparkOffsetManager.messageTrackers.get(this.solaceInputPartition.getId());
//                ConcurrentHashMap<String, SolaceMessage> messages = solaceMessageTracker.getMessages();
//                messages.remove(solaceRecord.getMessageId());
//                messages.put(solaceRecord.getMessageId(), solaceMessage);
//                solaceMessageTracker.setMessages(messages);
//
//                SolaceSparkOffsetManager.messageTrackers.put(this.solaceInputPartition.getId(), solaceMessageTracker);
//                log.info("SolaceSparkConnector - Updated existing message tracker");
//            } else {
//                SolaceMessageTracker solaceMessageTracker = new SolaceMessageTracker();
//                solaceMessageTracker.setClientName(Integer.toString(this.solaceInputPartition.getId()));
//                ConcurrentHashMap<String, SolaceMessage> messages = new ConcurrentHashMap<>();
//                messages.remove(solaceRecord.getMessageId());
//                messages.put(solaceRecord.getMessageId(), solaceMessage);
//                solaceMessageTracker.setMessages(messages);
//
//                SolaceSparkOffsetManager.messageTrackers.put(this.solaceInputPartition.getId(), solaceMessageTracker);
//                log.info("SolaceSparkConnector - Updated new message tracker");
//            }

            log.info("SolaceSparkConnector - Created Spark row for message with ID " + solaceRecord.getMessageId());
            return row;
        } catch (Exception e) {
            log.error("SolaceSparkConnector- Exception while reading message", e);
            throw new RuntimeException(e);
        }

//        log.error("SolaceSparkConnector- Exception while reading message");
//        return null;
    }

    private SolaceMessage getNextMessage() {
        LinkedBlockingQueue<SolaceMessage> queue = solaceBroker.getMessages(0);
        while(true) {
            try {
                solaceMessage = queue.poll(10000, TimeUnit.MILLISECONDS);
                if (solaceMessage == null) {
                    return null;
                } else {
                    if (!SolaceSparkOffsetManager.containsKey(solaceMessage.bytesXMLMessage.getReplicationGroupMessageId().toString())) {
                        log.info("SolaceSparkConnector - Message already processed. Skip to next message");
                    } else {
                        return solaceMessage;
                    }
                }
            } catch (InterruptedException e) {
                return null;
            }
        }
    }

    @Override
    public void close() {
        log.info("Input partition with ID " + this.solaceInputPartition.getId() + " is closed");
    }
}
