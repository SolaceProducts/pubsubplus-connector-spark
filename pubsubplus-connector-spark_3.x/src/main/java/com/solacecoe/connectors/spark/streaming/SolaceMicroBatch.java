package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkOffset;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class SolaceMicroBatch implements MicroBatchStream, SupportsAdmissionControl {
    private static Logger log = LogManager.getLogger(SolaceMicroBatch.class);
    private int latestOffsetValue = 0;
    private boolean isCommitTriggered = false;
    private String solaceOffsetIndicator = "MESSAGE_ID";
    private JsonObject offsetJson;
    private SolaceInputPartition[] inputPartitions;
//    private final CopyOnWriteArrayList<String> processedMessageIDs;
    private volatile ConcurrentHashMap<String, SolaceMessage> messages = new ConcurrentHashMap<>();
    private final int batchSize;
    private final int partitions;
    private final boolean ackLastProcessedMessages;
    private final boolean skipMessageReprocessingIfTasksAreRunningLate;
    private final boolean createFlowsOnSameSession;
    private final boolean includeHeaders;

    public SolaceMicroBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        log.info("SolaceSparkConnector - Initializing Solace Spark Connector");
        // Initialize classes required for Solace connectivity

        // User configuration validation
        if(!properties.containsKey("host") || properties.get("host") == null || properties.get("host").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Host name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(!properties.containsKey("vpn") || properties.get("vpn") == null || properties.get("vpn").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(!properties.containsKey("username") || properties.get("username") == null || properties.get("username").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Username in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Username in configuration options");
        }

        if(!properties.containsKey("password") || properties.get("password") == null || properties.get("password").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Password in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Password in configuration options");
        }

        if(!properties.containsKey("queue") || properties.get("queue") == null || properties.get("queue").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }

        if(!properties.containsKey("batchSize") || properties.get("batchSize") == null || properties.get("batchSize").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Batch size in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Batch Size in configuration options");
        }

        if(Integer.parseInt(properties.get("batchSize")) <= 0) {
            log.error("SolaceSparkConnector - Please set Batch size to minimum of 1");
            throw new RuntimeException("SolaceSparkConnector - Please set Batch size to minimum of 1");
        }

        ackLastProcessedMessages = properties.containsKey("ackLastProcessedMessages") && Boolean.parseBoolean(properties.get("ackLastProcessedMessages"));
        skipMessageReprocessingIfTasksAreRunningLate = properties.containsKey("skipDuplicates") && Boolean.parseBoolean(properties.get("skipDuplicates"));
        log.info("SolaceSparkConnector - Ack Last processed messages is set to {}", ackLastProcessedMessages);

        includeHeaders = properties.containsKey("includeHeaders") && Boolean.parseBoolean(properties.get("includeHeaders"));
        log.info("SolaceSparkConnector - includeHeaders is set to {}", includeHeaders);

        createFlowsOnSameSession = properties.containsKey("createFlowsOnSameSession") && Boolean.parseBoolean(properties.get("createFlowsOnSameSession"));
        log.info("SolaceSparkConnector - createFlowsOnSameSession is set to {}", createFlowsOnSameSession);

        batchSize = Integer.parseInt(properties.get("batchSize")) > 0 ? Integer.parseInt(properties.get("batchSize")) : 1;
        log.info("SolaceSparkConnector - Batch Size is set to {}", batchSize);

        partitions = properties.containsKey("partitions") ? Integer.parseInt(properties.get("partitions")) : 1;
        log.info("SolaceSparkConnector - Partitions is set to {}", partitions);
        inputPartitions = new SolaceInputPartition[partitions];

        if (properties.get("offsetIndicator") != null && !properties.get("offsetIndicator").isEmpty()) {
            this.solaceOffsetIndicator = properties.get("offsetIndicator");
            log.info("SolaceSparkConnector - offsetIndicator is set to {}", this.solaceOffsetIndicator);
        }

        log.info("SolaceSparkConnector - Solace Connection Details Host : " + properties.get("host") + ", VPN : " + properties.get("vpn") + ", Username : " + properties.get("username"));
        SolaceBroker solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"), properties);
        solaceConnectionManager.addConnection(solaceBroker);
        for (int i = 0; i < partitions; i++) {
            if(!createFlowsOnSameSession && i > 0) {
                solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"), properties);
                solaceConnectionManager.addConnection(solaceBroker);
            }
            EventListener eventListener = new EventListener((i + 1));
            // Initialize connection to Solace Broker
            solaceBroker.addReceiver(eventListener);
            log.info("SolaceSparkConnector - Acquired connection to Solace broker for partition {}", i);
        }

        this.offsetJson = new JsonObject();
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        latestOffsetValue+=batchSize;
//        log.info("SolaceSparkConnector - latestOffset :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return new SolaceSparkOffset(latestOffsetValue, String.join(",", this.messages.keySet().stream().collect(Collectors.toList())));
    }

    private boolean shouldAddMessage(String messageID) {
        return !skipMessageReprocessingIfTasksAreRunningLate || !this.messages.containsKey(messageID);
    }

    private InputPartition[] splitDataOnPartitions() {
        SolaceInputPartition[] partitions = new SolaceInputPartition[this.partitions];
        CopyOnWriteArrayList<SolaceRecord> recordList = new CopyOnWriteArrayList<>();
        for(int i=0; i < partitions.length; i++) {
            if(isCommitTriggered) {
                int brokerIndex = this.createFlowsOnSameSession ? 0 : i;
                SolaceBroker solaceBroker = SolaceConnectionManager.getConnection(brokerIndex);
                if(solaceBroker != null) {
                    int listenerIndex = this.createFlowsOnSameSession ? i : 0;
                    LinkedBlockingQueue<SolaceMessage> messages = solaceBroker.getMessages(listenerIndex);
                    log.info("SolaceSparkConnector - Creating new records list. Messages received from Solace " + messages.size());
                    recordList = new CopyOnWriteArrayList<>();
                    for (int j = 0; j < batchSize && j < messages.size(); j++) {
                        SolaceMessage solaceMessage = messages.poll();
                        assert solaceMessage != null;
                        BytesXMLMessage bytesXMLMessage = solaceMessage.bytesXMLMessage;
                        SolaceRecord solaceRecord = null;
                        try {
                            solaceRecord = SolaceRecord.getMapper(this.solaceOffsetIndicator).map(bytesXMLMessage);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        if (solaceRecord != null && shouldAddMessage(solaceRecord.getMessageId())) {
                            this.messages.put(solaceRecord.getMessageId(), solaceMessage);
                            if (ackLastProcessedMessages) {
                                log.info("SolaceSparkConnector - Ack last processed messages is enabled. Checking if message is already processed based on available offsets.");
                                // based on last successful offset, extract the message ID and see if same message is received, if so ack the message
                                if (offsetJson != null && offsetJson.has("messageIDs")) {
                                    List<String> messageIDsInLastOffset = Arrays.asList(offsetJson.get("messageIDs").getAsString().split(","));
                                    log.info("SolaceSparkConnector - Total messages in offset :: " + messageIDsInLastOffset.size());
                                    if (messageIDsInLastOffset.contains(solaceRecord.getMessageId())) {
                                        log.info("SolaceSparkConnector - Message found in offset. Acknowledging previously processed message with ID :: " + solaceRecord.getMessageId());
                                        bytesXMLMessage.ackMessage();

                                        this.messages.remove(solaceRecord.getMessageId());
                                    } else {
//                                        if(!this.messages.contains(solaceRecord.getMessageId())) {
//                                            log.info("SolaceSparkConnector - Message is not present in offset. Hence reprocessing it.");
//                                            this.messages.put(solaceRecord.getMessageId(), solaceMessage);
//                                            recordList.add(solaceRecord);
//                                        }
                                        log.info("SolaceSparkConnector - Message is not present in offset. Hence reprocessing it.");
                                        recordList.add(solaceRecord);
                                    }
                                } else {
//                                    if(!this.messages.contains(solaceRecord.getMessageId())) {
//                                        log.info("SolaceSparkConnector - Trying to check if messages are already processed but offset is not available. Hence reprocessing it.");
//                                        this.messages.put(solaceRecord.getMessageId(), solaceMessage);
//                                        recordList.add(solaceRecord);
//                                    }
                                    log.info("SolaceSparkConnector - Trying to check if messages are already processed but offset is not available. Hence reprocessing it.");
                                    recordList.add(solaceRecord);
                                }
                            } else {
//                                if(!this.processedMessageIDs.contains(solaceRecord.getMessageId())) {
//                                    this.processedMessageIDs.add(solaceRecord.getMessageId());
//                                    recordList.add(solaceRecord);
//                                }
                                recordList.add(solaceRecord);
                            }

                        }
                    }
                }
            } else if(inputPartitions != null && inputPartitions.length > 0 && inputPartitions[i] != null) {
                recordList = inputPartitions[i].getValues();
            }

            log.info("SolaceSparkConnector - Plan input partitions with records :: " + recordList.size());
            partitions[i] = new SolaceInputPartition(i,"", recordList);
        }

        if(isCommitTriggered) {
            isCommitTriggered = false;
        }

        inputPartitions = partitions;

        return partitions;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        return splitDataOnPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.info("SolaceSparkConnector - Create reader factory with includeHeaders :: " + this.includeHeaders);
        return new SolaceDataSourceReaderFactory(this.includeHeaders);
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        latestOffsetValue+=batchSize;
//        log.info("SolaceSparkConnector - latestOffset with params :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return new SolaceSparkOffset(latestOffsetValue, String.join(",", this.messages.keySet().stream().collect(Collectors.toList())));
    }

    @Override
    public Offset initialOffset() {
        return new SolaceSparkOffset(latestOffsetValue, String.join(",", this.messages.keySet().stream().collect(Collectors.toList())));
    }

    @Override
    public Offset deserializeOffset(String json) {
        JsonObject gson = new Gson().fromJson(json, JsonObject.class);
        if(gson != null && gson.has("offset")) {
            latestOffsetValue = gson.get("offset").getAsInt();
            offsetJson = gson;
        }

//        if(gson != null && gson.has("messageIDs")) {
//            String messageIDs = gson.get("messageIDs").getAsString();
//            if(messageIDs.length() > 0) {
//                this.processedMessageIDs.addAll(Arrays.asList(messageIDs.split(",")));
//            }
//        }
        return new SolaceSparkOffset(latestOffsetValue, String.join(",",this.messages.keySet().stream().collect(Collectors.toList())));
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
        SolaceSparkOffset solaceSparkOffset = (SolaceSparkOffset) end;
//        log.info("SolaceSparkConnector - Processed message ID's by Spark " + basicOffset.json());

        if (solaceSparkOffset != null && solaceSparkOffset.getMessageIDs() != null && !solaceSparkOffset.getMessageIDs().isEmpty()) {
            String[] messageIDs = solaceSparkOffset.getMessageIDs().split(",");
            for (String messageID : messageIDs) {
                if (this.messages.containsKey(messageID)) {
                    this.messages.get(messageID).bytesXMLMessage.ackMessage();
                    log.info("SolaceSparkConnector - Acknowledged message with ID :: {}", messageID);
                    this.messages.remove(messageID);
//                    if (this.processedMessageIDs.contains(messageID)) {
//                        this.processedMessageIDs.remove(messageID);
//                    }
                }
            }
        }


        isCommitTriggered = true;
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing connection to Solace");
    }

}
