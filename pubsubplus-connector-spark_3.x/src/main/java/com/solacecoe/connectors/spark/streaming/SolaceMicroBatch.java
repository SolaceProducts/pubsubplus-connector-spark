package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkOffset;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.*;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class SolaceMicroBatch implements MicroBatchStream, SupportsAdmissionControl {
    private static Logger log = LoggerFactory.getLogger(SolaceMicroBatch.class);
    private int latestOffsetValue = 0;
    private boolean isCommitTriggered = false;
    private String solaceOffsetIndicator = "MESSAGE_ID";
    private JsonObject offsetJson;
    private SolaceInputPartition[] inputPartitions;
    private final SolaceConnectionManager solaceConnectionManager;
//    private final CopyOnWriteArrayList<String> processedMessageIDs;
    private final ConcurrentHashMap<String, SolaceMessage> messages;
    private final int batchSize;
    private final int partitions;
    private final boolean ackLastProcessedMessages;
    private final boolean skipMessageReprocessingIfTasksAreRunningLate;
    private final boolean createFlowsOnSameSession;
    private final boolean includeHeaders;
    private SolaceBroker lvqConnection;
    private boolean initialOffset = false;
    private String lvqTopic;
    private final String checkpointType;
    public SolaceMicroBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        log.info("SolaceSparkConnector - Initializing Solace Spark Connector");
        // Initialize classes required for Solace connectivity

        // User configuration validation
        if(!properties.containsKey(SolaceSparkStreamingProperties.HOST) || properties.get(SolaceSparkStreamingProperties.HOST) == null || properties.get(SolaceSparkStreamingProperties.HOST).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Host name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(!properties.containsKey(SolaceSparkStreamingProperties.VPN) || properties.get(SolaceSparkStreamingProperties.VPN) == null || properties.get(SolaceSparkStreamingProperties.VPN).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME) &&
                properties.get(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME).equals(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)) {
            if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
                throw new RuntimeException("SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
            }

            if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide OAuth Client ID");
                throw new RuntimeException("SolaceSparkConnector - Please provide OAuth Client ID");
            }

            if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
                throw new RuntimeException("SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
            }
        } else {
            if (!properties.containsKey(SolaceSparkStreamingProperties.USERNAME) || properties.get(SolaceSparkStreamingProperties.USERNAME) == null || properties.get(SolaceSparkStreamingProperties.USERNAME).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide Solace Username in configuration options");
                throw new RuntimeException("SolaceSparkConnector - Please provide Solace Username in configuration options");
            }

            if (!properties.containsKey(SolaceSparkStreamingProperties.PASSWORD) || properties.get(SolaceSparkStreamingProperties.PASSWORD) == null || properties.get(SolaceSparkStreamingProperties.PASSWORD).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide Solace Password in configuration options");
                throw new RuntimeException("SolaceSparkConnector - Please provide Solace Password in configuration options");
            }
        }

        if(!properties.containsKey(SolaceSparkStreamingProperties.QUEUE) || properties.get(SolaceSparkStreamingProperties.QUEUE) == null || properties.get(SolaceSparkStreamingProperties.QUEUE).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }

        if(!properties.containsKey(SolaceSparkStreamingProperties.BATCH_SIZE) || properties.get(SolaceSparkStreamingProperties.BATCH_SIZE) == null || properties.get(SolaceSparkStreamingProperties.BATCH_SIZE).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Batch size in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Batch Size in configuration options");
        }

        if(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.BATCH_SIZE)) <= 0) {
            log.error("SolaceSparkConnector - Please set Batch size to minimum of 1");
            throw new RuntimeException("SolaceSparkConnector - Please set Batch size to minimum of 1");
        }

        ackLastProcessedMessages = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES, SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES_DEFAULT));
        skipMessageReprocessingIfTasksAreRunningLate = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.SKIP_DUPLICATES, SolaceSparkStreamingProperties.SKIP_DUPLICATES_DEFAULT));
        log.info("SolaceSparkConnector - Ack Last processed messages is set to " + ackLastProcessedMessages);

        includeHeaders = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        log.info("SolaceSparkConnector - includeHeaders is set to " + includeHeaders);

        createFlowsOnSameSession = properties.containsKey("createFlowsOnSameSession") ? Boolean.valueOf(properties.get("createFlowsOnSameSession")) : false;
        log.info("SolaceSparkConnector - createFlowsOnSameSession is set to " + createFlowsOnSameSession);

        batchSize = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT));
        log.info("SolaceSparkConnector - Batch Size is set to " + batchSize);

        partitions = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.PARTITIONS, SolaceSparkStreamingProperties.PARTITIONS_DEFAULT));
        log.info("SolaceSparkConnector - Partitions is set to " + partitions);
        inputPartitions = new SolaceInputPartition[partitions];

        this.solaceOffsetIndicator = properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT);
        log.info("SolaceSparkConnector - offsetIndicator is set to " + this.solaceOffsetIndicator);

        solaceConnectionManager = new SolaceConnectionManager();
        log.info("SolaceSparkConnector - Solace Connection Details Host : " + properties.get(SolaceSparkStreamingProperties.HOST) + ", VPN : " + properties.get(SolaceSparkStreamingProperties.VPN) + ", Username : " + properties.get(SolaceSparkStreamingProperties.USERNAME));
        SolaceBroker solaceBroker = new SolaceBroker(properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME), properties.get(SolaceSparkStreamingProperties.PASSWORD), properties.get(SolaceSparkStreamingProperties.QUEUE), properties);
        solaceConnectionManager.addConnection(solaceBroker);
        for (int i = 0; i < partitions; i++) {
            if(!createFlowsOnSameSession && i > 0) {
                solaceBroker = new SolaceBroker(properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME), properties.get(SolaceSparkStreamingProperties.PASSWORD), properties.get(SolaceSparkStreamingProperties.QUEUE), properties);
                solaceConnectionManager.addConnection(solaceBroker);
            }
            EventListener eventListener = new EventListener((i + 1), properties.getOrDefault(SolaceSparkStreamingProperties.LAST_SUCCESSFUL_MESSAGE_ID, null),
                    Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.LAST_SUCCESSFUL_MESSAGE_ID_THRESHOLD, Integer.toString(batchSize))), solaceOffsetIndicator);
            // Initialize connection to Solace Broker
            solaceBroker.addReceiver(eventListener);

            log.info("SolaceSparkConnector - Acquired connection to Solace broker for partition " + i);
        }

        checkpointType = properties.getOrDefault(SolaceSparkStreamingProperties.CHECKPOINT_TYPE, SolaceSparkStreamingProperties.CHECKPOINT_TYPE_DEFAULT);
        if(checkpointType.equals("lvq")) {
            lvqTopic = properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC);
            lvqConnection = new SolaceBroker(properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME), properties.get(SolaceSparkStreamingProperties.PASSWORD), properties.get(SolaceSparkStreamingProperties.QUEUE), properties);
            LVQEventListener lvqEventListener = new LVQEventListener();
            lvqConnection.addLVQReceiver(lvqEventListener);
            solaceConnectionManager.addLVQConnection(lvqConnection);
        }

        this.messages = new ConcurrentHashMap<>();
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
                SolaceBroker solaceBroker = solaceConnectionManager.getConnection(brokerIndex);
                if(solaceBroker != null) {
                    int listenerIndex = this.createFlowsOnSameSession ? i : 0;
                    ConcurrentLinkedQueue<SolaceMessage> messages = solaceBroker.getMessages(listenerIndex);
                    log.info("SolaceSparkConnector - Creating new records list. Messages received from Solace {}", messages.size());
                    recordList = new CopyOnWriteArrayList<>();
                    for (int j = 0; j < batchSize && j < messages.size(); j++) {
                        SolaceMessage solaceMessage = messages.poll();
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
                                    log.info("SolaceSparkConnector - Total messages in offset :: {}", messageIDsInLastOffset.size());
                                    if (messageIDsInLastOffset.contains(solaceRecord.getMessageId())) {
                                        log.info("SolaceSparkConnector - Message found in offset. Acknowledging previously processed message with ID :: {}", solaceRecord.getMessageId());
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
        this.initialOffset = true;
        if(checkpointType.equals("lvq") && this.lvqConnection != null) {
            SolaceSparkOffset solaceSparkOffset = this.lvqConnection.getLVQMessage();
            if (solaceSparkOffset != null) {
                latestOffsetValue = solaceSparkOffset.getOffset();
                return new SolaceSparkOffset(solaceSparkOffset.getOffset(), solaceSparkOffset.getMessageIDs());
            }
        }
        return new SolaceSparkOffset(latestOffsetValue, String.join(",", this.messages.keySet().stream().collect(Collectors.toList())));
    }

    @Override
    public Offset deserializeOffset(String json) {
        JsonObject gson = new Gson().fromJson(json, JsonObject.class);
        if(gson != null && gson.has("offset")) {
            latestOffsetValue = gson.get("offset").getAsInt();
            offsetJson = gson;
        }

        if(!initialOffset && checkpointType.equals("lvq") && this.lvqConnection != null) {
            SolaceSparkOffset solaceSparkOffset = this.lvqConnection.getLVQMessage();
            if (solaceSparkOffset != null) {
                latestOffsetValue = solaceSparkOffset.getOffset();
                offsetJson = new Gson().fromJson(solaceSparkOffset.json(), JsonObject.class);
                return new SolaceSparkOffset(solaceSparkOffset.getOffset(), solaceSparkOffset.getMessageIDs());
            }
        }

        return new SolaceSparkOffset(latestOffsetValue, String.join(",", new ArrayList<>(this.messages.keySet())));
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
        SolaceSparkOffset solaceSparkOffset = (SolaceSparkOffset) end;
//        log.info("SolaceSparkConnector - Processed message ID's by Spark " + basicOffset.json());

        if(solaceSparkOffset != null && solaceSparkOffset.getMessageIDs() != null && !solaceSparkOffset.getMessageIDs().isEmpty()) {
            String[] messageIDs = solaceSparkOffset.getMessageIDs().split(",");
            for(String messageID: messageIDs) {
                if (this.messages.containsKey(messageID)) {
                    this.messages.get(messageID).bytesXMLMessage.ackMessage();
                    log.info("SolaceSparkConnector - Acknowledged message with ID :: {}", messageID);
                    this.messages.remove(messageID);
//                    if (this.processedMessageIDs.contains(messageID)) {
//                        this.processedMessageIDs.remove(messageID);
//                    }
                }
            }
            if(messageIDs.length > 0 && checkpointType.equals("lvq") && this.lvqConnection != null) {
                this.lvqConnection.publishMessage(this.lvqTopic, solaceSparkOffset.json());
            }
        }

        isCommitTriggered = true;
        log.info("SolaceSparkConnector - Commit is true");
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing connection to Solace");
        if(solaceConnectionManager != null) {
            solaceConnectionManager.close();
        }
    }

}
