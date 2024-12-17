package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidPropertyException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceRecordMapperException;
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
    private static final Logger log = LoggerFactory.getLogger(SolaceMicroBatch.class);
    private int latestOffsetValue = 0;
    private boolean isCommitTriggered = false;
    private final String solaceOffsetIndicator;
    private JsonObject offsetJson;
    private SolaceInputPartition[] inputPartitions;
    private final SolaceConnectionManager solaceConnectionManager;
    private final ConcurrentHashMap<String, SolaceMessage> messages;
    private final CopyOnWriteArrayList<String> committedMessages;
    private final int batchSize;
    private final int partitions;
    private final boolean ackLastProcessedMessages;
    private final boolean skipMessageReprocessingIfTasksAreRunningLate;
    private final boolean createFlowsOnSameSession;
    private final boolean includeHeaders;
    private final boolean initiateReplay;

    public SolaceMicroBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        log.info("SolaceSparkConnector - Initializing Solace Spark Connector");
        // Initialize classes required for Solace connectivity

        // User configuration validation
        if(!properties.containsKey(SolaceSparkStreamingProperties.HOST) || properties.get(SolaceSparkStreamingProperties.HOST) == null || properties.get(SolaceSparkStreamingProperties.HOST).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Host name in configuration options");
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(!properties.containsKey(SolaceSparkStreamingProperties.VPN) || properties.get(SolaceSparkStreamingProperties.VPN) == null || properties.get(SolaceSparkStreamingProperties.VPN).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME) &&
                properties.get(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME).equals(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)) {
            if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN)) {
                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL).isEmpty()) {
                    log.error("SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
                }

                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID).isEmpty()) {
                    log.error("SolaceSparkConnector - Please provide OAuth Client ID");
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client ID");
                }

                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET).isEmpty()) {
                    log.error("SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
                }

                String trustStoreFilePassword = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, null);
                if(trustStoreFilePassword == null || trustStoreFilePassword.isEmpty()) {
                    log.error("SolaceSparkConnector - Please provide OAuth Client TrustStore Password. If TrustStore file path is not configured, please provide password for default java truststore");
                }
            } else if(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, null) == null || properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, null).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide valid access token input");
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide valid access token input");
            }
        } else {
            if (!properties.containsKey(SolaceSparkStreamingProperties.USERNAME) || properties.get(SolaceSparkStreamingProperties.USERNAME) == null || properties.get(SolaceSparkStreamingProperties.USERNAME).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide Solace Username in configuration options");
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Username in configuration options");
            }

            if (!properties.containsKey(SolaceSparkStreamingProperties.PASSWORD) || properties.get(SolaceSparkStreamingProperties.PASSWORD) == null || properties.get(SolaceSparkStreamingProperties.PASSWORD).isEmpty()) {
                log.error("SolaceSparkConnector - Please provide Solace Password in configuration options");
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Password in configuration options");
            }
        }

        if(!properties.containsKey(SolaceSparkStreamingProperties.QUEUE) || properties.get(SolaceSparkStreamingProperties.QUEUE) == null || properties.get(SolaceSparkStreamingProperties.QUEUE).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }

        if(!properties.containsKey(SolaceSparkStreamingProperties.BATCH_SIZE) || properties.get(SolaceSparkStreamingProperties.BATCH_SIZE) == null || properties.get(SolaceSparkStreamingProperties.BATCH_SIZE).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Batch size in configuration options");
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Batch Size in configuration options");
        }

        if(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.BATCH_SIZE)) <= 0) {
            log.error("SolaceSparkConnector - Please set Batch size to minimum of 1");
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please set Batch size to minimum of 1");
        }

        ackLastProcessedMessages = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES, SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES_DEFAULT));
        skipMessageReprocessingIfTasksAreRunningLate = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.SKIP_DUPLICATES, SolaceSparkStreamingProperties.SKIP_DUPLICATES_DEFAULT));
        log.info("SolaceSparkConnector - Ack Last processed messages is set to {}", ackLastProcessedMessages);

        includeHeaders = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        log.info("SolaceSparkConnector - includeHeaders is set to {}", includeHeaders);

        createFlowsOnSameSession = Boolean.parseBoolean(properties.getOrDefault("createFlowsOnSameSession", "false"));
        log.info("SolaceSparkConnector - createFlowsOnSameSession is set to {}", createFlowsOnSameSession);

        batchSize = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT));
        log.info("SolaceSparkConnector - Batch Size is set to {}", batchSize);

        partitions = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.PARTITIONS, SolaceSparkStreamingProperties.PARTITIONS_DEFAULT));
        log.info("SolaceSparkConnector - Partitions is set to {}", partitions);
        inputPartitions = new SolaceInputPartition[partitions];

        this.solaceOffsetIndicator = properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT);
        log.info("SolaceSparkConnector - offsetIndicator is set to {}", this.solaceOffsetIndicator);

        solaceConnectionManager = new SolaceConnectionManager();
        log.info("SolaceSparkConnector - Solace Connection Details Host : {}, VPN : {}, Username : {}", properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME));
        this.initiateReplay = properties.getOrDefault(SolaceSparkStreamingProperties.REPLAY_STRATEGY ,null) != null;
        SolaceBroker solaceBroker = new SolaceBroker(properties, initiateReplay);
        solaceConnectionManager.addConnection(solaceBroker);
        for (int i = 0; i < partitions; i++) {
            if(!createFlowsOnSameSession && i > 0) {
                solaceBroker = new SolaceBroker(properties, false);
                solaceConnectionManager.addConnection(solaceBroker);
            }
            EventListener eventListener = new EventListener((i + 1));
            // Initialize connection to Solace Broker
            solaceBroker.addReceiver(eventListener);
            log.info("SolaceSparkConnector - Acquired connection to Solace broker for partition {}", i);
        }

        this.messages = new ConcurrentHashMap<>();
        this.committedMessages = new CopyOnWriteArrayList<>();
        this.offsetJson = new JsonObject();
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        checkSolaceException();
        latestOffsetValue+=batchSize;
        return new BasicOffset(latestOffsetValue, String.join(",", this.messages.keySet().stream().collect(Collectors.toList())));
    }

    private boolean shouldAddMessage(String messageID) {
        if(this.messages.containsKey(messageID)) {
            log.info("SolaceSparkConnector - Duplicate message received {}", messageID);
        }
        return !this.messages.containsKey(messageID);
    }

    private InputPartition[] splitDataOnPartitions() {
        SolaceInputPartition[] solaceInputPartitions = new SolaceInputPartition[this.partitions];
        CopyOnWriteArrayList<SolaceRecord> recordList = new CopyOnWriteArrayList<>();
        for(int i=0; i < solaceInputPartitions.length; i++) {
            if(isCommitTriggered) {
                int brokerIndex = this.createFlowsOnSameSession ? 0 : i;
                SolaceBroker solaceBroker = solaceConnectionManager.getConnection(brokerIndex);
                if(solaceBroker != null) {
                    int listenerIndex = this.createFlowsOnSameSession ? i : 0;
                    ConcurrentLinkedQueue<SolaceMessage> currentMessages = solaceBroker.getMessages(listenerIndex);
                    log.info("SolaceSparkConnector - Creating new records list. Messages received from Solace {}", currentMessages.size());
                    recordList = new CopyOnWriteArrayList<>();
                    for (int j = 0; j < batchSize && j < currentMessages.size(); j++) {
                        SolaceMessage solaceMessage = currentMessages.poll();
                        BytesXMLMessage bytesXMLMessage = solaceMessage.bytesXMLMessage;
                        SolaceRecord solaceRecord;
                        try {
                            solaceRecord = SolaceRecord.getMapper(this.solaceOffsetIndicator).map(bytesXMLMessage);
                        } catch (Exception e) {
                            throw new SolaceRecordMapperException(e);
                        }

                        if (solaceRecord != null) {
                            if(this.committedMessages.contains(solaceRecord.getMessageId())) {
                                log.info("SolaceSparkConnector - Is Message redelivered {}. Acknowledging message with id {} as it is already processed", solaceRecord.isRedelivered(), solaceRecord.getMessageId());
                                bytesXMLMessage.ackMessage();
                                this.committedMessages.remove(solaceRecord.getMessageId());
                            } else if(this.messages.containsKey(solaceRecord.getMessageId())) {
                                // update with latest message so that ack is processed correctly. No need to add to input partition as it is already added.
                                this.messages.put(solaceRecord.getMessageId(), solaceMessage);
                            } else {
                                this.messages.put(solaceRecord.getMessageId(), solaceMessage);
                                if (ackLastProcessedMessages && !this.initiateReplay) {
                                    log.info("SolaceSparkConnector - Ack last processed messages is enabled. Checking if message is already processed based on available offsets.");
                                    // based on last successful offset, extract the message ID and see if same message is received, if so ack the message
                                    if (offsetJson != null && offsetJson.has("messageIDs")) {
                                        List<String> messageIDsInLastOffset = Arrays.asList(offsetJson.get("messageIDs").getAsString().split(","));
                                        log.info("SolaceSparkConnector - Total messages in offset :: {}", messageIDsInLastOffset.size());
                                        if (messageIDsInLastOffset.contains(solaceRecord.getMessageId())) {
                                            log.info("SolaceSparkConnector - Message id found in offset. Acknowledging message with id {}", solaceRecord.getMessageId());
                                            bytesXMLMessage.ackMessage();

                                            this.messages.remove(solaceRecord.getMessageId());
                                        } else {
                                            log.info("SolaceSparkConnector - Message id is not present in offset. Hence reprocessing it.");
                                            recordList.add(solaceRecord);
                                        }
                                    } else {
                                        log.info("SolaceSparkConnector - No offset is available. Hence reprocessing it.");
                                        recordList.add(solaceRecord);
                                    }
                                } else {
                                    recordList.add(solaceRecord);
                                }
                            }
                        }
                    }
                }
            } else if(inputPartitions != null && inputPartitions.length > 0 && inputPartitions[i] != null) {
                recordList = inputPartitions[i].getValues();
            }

            log.info("SolaceSparkConnector - Creating input partitions with {} records", recordList.size());
            solaceInputPartitions[i] = new SolaceInputPartition(i,"", recordList);
        }

        if(isCommitTriggered) {
            isCommitTriggered = false;
        }

        inputPartitions = solaceInputPartitions;

        return solaceInputPartitions;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        checkSolaceException();
        return splitDataOnPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        checkSolaceException();
        log.info("SolaceSparkConnector - Create reader factory with includeHeaders set to {}", this.includeHeaders);
        return new SolaceDataSourceReaderFactory(this.includeHeaders);
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        checkSolaceException();
        latestOffsetValue+=batchSize;
        return new BasicOffset(latestOffsetValue, String.join(",", this.messages.keySet().stream().collect(Collectors.toList())));
    }

    @Override
    public Offset initialOffset() {
        checkSolaceException();
        return new BasicOffset(latestOffsetValue, String.join(",", this.messages.keySet().stream().collect(Collectors.toList())));
    }

    @Override
    public Offset deserializeOffset(String json) {
        checkSolaceException();
        JsonObject gson = new Gson().fromJson(json, JsonObject.class);
        if(gson != null && gson.has("offset")) {
            log.info("SolaceSparkConnector - Found previous offset with id {} and is messageIDs present {}", gson.get("offset").getAsInt(), gson.has("messageIDs"));
            latestOffsetValue = gson.get("offset").getAsInt();
            offsetJson = gson;
        }
        return new BasicOffset(latestOffsetValue, String.join(",",this.messages.keySet().stream().collect(Collectors.toList())));
    }

    @Override
    public void commit(Offset end) {
        checkSolaceException();
        log.info("SolaceSparkConnector - Commit triggered. Starting to acknowledge messages");
        BasicOffset basicOffset = (BasicOffset) end;
        long startTime = System.currentTimeMillis();
        if(basicOffset != null && basicOffset.getMessageIDs() != null && !basicOffset.getMessageIDs().isEmpty()) {
            String[] messageIDs = basicOffset.getMessageIDs().split(",");
            for(String messageID: messageIDs) {
                if (this.messages.containsKey(messageID)) {
                    this.messages.get(messageID).bytesXMLMessage.ackMessage();
                    if(log.isDebugEnabled()) {
                        log.debug("SolaceSparkConnector - Acknowledged message with ID :: {}", messageID);
                    }
                    this.messages.remove(messageID);
                    this.committedMessages.add(messageID);
                }
            }

            log.info("SolaceSparkConnector - Message acknowledgements completed.");
            if(log.isDebugEnabled()) {
                log.debug("SolaceSparkConnector - Time taken to acknowledge {} messages :: {}", messageIDs.length, System.currentTimeMillis() - startTime);
            }
        }

        isCommitTriggered = true;
    }

    public void checkSolaceException() {
        solaceConnectionManager.getConnections().forEach(solaceBroker -> {
            if(solaceBroker.isException()) {
                throw new RuntimeException(solaceBroker.getException());
            }
        });
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing connection to Solace");
        if(solaceConnectionManager != null) {
            solaceConnectionManager.close();
        }
    }

}
