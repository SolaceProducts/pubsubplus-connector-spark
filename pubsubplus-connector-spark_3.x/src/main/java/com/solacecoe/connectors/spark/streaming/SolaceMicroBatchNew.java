package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.offset.SolaceSparkOffset;
import com.solacecoe.connectors.spark.offset.SolaceSparkOffsetManager;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceDataSourceReaderFactoryNew;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceInputPartitionNew;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
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

import java.util.Map;
import java.util.concurrent.*;

public class SolaceMicroBatchNew implements MicroBatchStream, SupportsAdmissionControl {
    private static Logger log = LogManager.getLogger(SolaceMicroBatchNew.class);
    private int latestOffsetValue = 0;
    private boolean isCommitTriggered = false;
    private String solaceOffsetIndicator = "MESSAGE_ID";
    private JsonObject offsetJson;
//    private final SolaceConnectionManager solaceConnectionManager;
//    private final CopyOnWriteArrayList<String> processedMessageIDs;
    private SolaceInputPartitionNew[] inputPartitions;
    private final int batchSize;
    private final int partitions;
    private final boolean ackLastProcessedMessages;
    private final boolean skipMessageReprocessingIfTasksAreRunningLate;
    private final boolean createFlowsOnSameSession;
    private final boolean includeHeaders;
    private final boolean asyncAck;
    private final Map<String, String> properties;
    private ExecutorService executorService;
    private SolaceConnectionManager solaceConnectionManager;

    public SolaceMicroBatchNew(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.properties = properties;
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
        inputPartitions = new SolaceInputPartitionNew[partitions];

        asyncAck = properties.containsKey("asyncAck") && Boolean.parseBoolean(properties.get("asyncAck"));
        log.info("SolaceSparkConnector - asyncAck is set to {}", asyncAck);

        if (properties.get("offsetIndicator") != null && !properties.get("offsetIndicator").isEmpty()) {
            this.solaceOffsetIndicator = properties.get("offsetIndicator");
            log.info("SolaceSparkConnector - offsetIndicator is set to {}", this.solaceOffsetIndicator);
        }

//        solaceConnectionManager = new SolaceConnectionManager();
//        log.info("SolaceSparkConnector - Solace Connection Details Host : " + properties.get("host") + ", VPN : " + properties.get("vpn") + ", Username : " + properties.get("username"));
//        SolaceBroker solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"));
//        solaceConnectionManager.addConnection(solaceBroker);
//        for (int i = 0; i < partitions; i++) {
//            if(!createFlowsOnSameSession && i > 0) {
//                solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"));
//                solaceConnectionManager.addConnection(solaceBroker);
//            }
//            EventListener eventListener = new EventListener((i + 1));
//            // Initialize connection to Solace Broker
//            solaceBroker.addReceiver(eventListener);
//            log.info("SolaceSparkConnector - Acquired connection to Solace broker for partition {}", i);
//        }

        this.offsetJson = new JsonObject();
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        latestOffsetValue+=batchSize;
//        log.info("SolaceSparkConnector - latestOffset :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return new SolaceSparkOffset(latestOffsetValue, SolaceSparkOffsetManager.getProcessedMessages());
    }

    private InputPartition[] splitDataOnPartitions() {
        for(int i=0; i < this.partitions; i++) {
            inputPartitions[i] = new SolaceInputPartitionNew(i, "", this.properties);
        }

        if(isCommitTriggered) {
            isCommitTriggered = false;
        }
        return inputPartitions;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        return splitDataOnPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.info("SolaceSparkConnector - Create reader factory with includeHeaders :: " + this.includeHeaders);
        return new SolaceDataSourceReaderFactoryNew(this.includeHeaders, this.properties);
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        latestOffsetValue+=batchSize;
//        log.info("SolaceSparkConnector - latestOffset with params :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return new SolaceSparkOffset(latestOffsetValue, SolaceSparkOffsetManager.getProcessedMessages());
    }

    @Override
    public Offset initialOffset() {
        return new SolaceSparkOffset(latestOffsetValue, SolaceSparkOffsetManager.getProcessedMessages());
    }

    @Override
    public Offset deserializeOffset(String json) {
        JsonObject gson = new Gson().fromJson(json, JsonObject.class);
        if(gson != null && gson.has("offset")) {
            latestOffsetValue = gson.get("offset").getAsInt();
            offsetJson = gson;
        }

        return new SolaceSparkOffset(latestOffsetValue, SolaceSparkOffsetManager.getProcessedMessages());
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
        SolaceSparkOffset solaceSparkOffset = (SolaceSparkOffset) end;
//        log.info("SolaceSparkConnector - Processed message ID's by Spark " + basicOffset.json());

        if (solaceSparkOffset != null && solaceSparkOffset.getMessageIDs() != null && !solaceSparkOffset.getMessageIDs().isEmpty()) {
            String[] messageIDs = solaceSparkOffset.getMessageIDs().split(",");
            for(int i=0; i<partitions; i++) {
                ConcurrentHashMap<String, SolaceMessage> messages = SolaceSparkOffsetManager.getMessages();
                if(messages != null) {
                    for (String messageID : messageIDs) {
                        if (messages != null && messages.containsKey(messageID)) {
                            log.info("SolaceSparkConnector - Acknowledging message with ID :: {}", messageID);
                            messages.get(messageID).bytesXMLMessage.ackMessage();
                            log.info("SolaceSparkConnector - Acknowledged message with ID :: {}", messageID);
                            messages.remove(messageID);
                        }
                    }
                }
            }
        }

        isCommitTriggered = true;
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing");
        SolaceConnectionManager.close();
    }

}
