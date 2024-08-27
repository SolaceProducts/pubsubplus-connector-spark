package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.offset.SolaceSparkOffset;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceDataSourceReaderFactoryNew;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceInputPartitionNew;
import com.solacecoe.connectors.spark.streaming.solace.LVQEventListener;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
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

public class SolaceMicroBatchNew implements MicroBatchStream, SupportsAdmissionControl {
    private static final Logger log = LogManager.getLogger(SolaceMicroBatchNew.class);
    private int latestOffsetValue = 0;
    private final SolaceInputPartitionNew[] inputPartitions;
    private final int partitions;
    private final int batchSize;
    private final boolean includeHeaders;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;

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

        this.batchSize = Integer.parseInt(properties.getOrDefault("batchSize", "0"));
        includeHeaders = Boolean.parseBoolean(properties.getOrDefault("includeHeaders", "false"));
        log.info("SolaceSparkConnector - includeHeaders is set to {}", includeHeaders);

        partitions = Integer.parseInt(properties.getOrDefault("partitions", "1"));
        log.info("SolaceSparkConnector - Partitions is set to {}", partitions);
        inputPartitions = new SolaceInputPartitionNew[partitions];

        String solaceOffsetIndicator = properties.getOrDefault("offsetIndicator", "MESSAGE_ID");
        log.info("SolaceSparkConnector - offsetIndicator is set to {}", solaceOffsetIndicator);

        this.solaceBroker = new SolaceBroker(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"));
        LVQEventListener lvqEventListener = new LVQEventListener();
        this.solaceBroker.addLVQReceiver(lvqEventListener);
        SolaceConnectionManager.addConnection(0, this.solaceBroker);
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        latestOffsetValue+=batchSize;
//        log.info("SolaceSparkConnector - latestOffset :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return new SolaceSparkOffset(latestOffsetValue, "");
    }

    private InputPartition[] getPartitions() {
        for(int i=0; i < this.partitions; i++) {
//            /solaceSparkOffsetManager.messageIDs = offsetJson != null && offsetJson.has("messageIDs") ? Arrays.asList(offsetJson.get("messageIDs").getAsString().split(",")) : new ArrayList<>();
            inputPartitions[i] = new SolaceInputPartitionNew(i, "");
        }
        return inputPartitions;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        return getPartitions();
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.info("SolaceSparkConnector - Create reader factory with includeHeaders :: " + this.includeHeaders);
        return new SolaceDataSourceReaderFactoryNew(this.includeHeaders, this.properties);
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        latestOffsetValue+=batchSize;
        SolaceSparkOffset solaceSparkOffset = this.solaceBroker.getLVQMessage();
        if(solaceSparkOffset != null) {
            return new SolaceSparkOffset(latestOffsetValue, solaceSparkOffset.getMessageIDs());
        }
//        log.info("SolaceSparkConnector - latestOffset with params :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return new SolaceSparkOffset(latestOffsetValue, "");
    }

    @Override
    public Offset initialOffset() {
        SolaceSparkOffset solaceSparkOffset = this.solaceBroker.getLVQMessage();
        if(solaceSparkOffset != null) {
            return new SolaceSparkOffset(solaceSparkOffset.getOffset(), solaceSparkOffset.getMessageIDs());
        }
        return new SolaceSparkOffset(latestOffsetValue, "");
    }

    @Override
    public Offset deserializeOffset(String json) {
        JsonObject gson = new Gson().fromJson(json, JsonObject.class);
        if(gson != null && gson.has("offset")) {
            latestOffsetValue = gson.get("offset").getAsInt();
        }

        SolaceSparkOffset solaceSparkOffset = this.solaceBroker.getLVQMessage();
        if(solaceSparkOffset != null) {
            return new SolaceSparkOffset(latestOffsetValue, solaceSparkOffset.getMessageIDs());
        }

        return new SolaceSparkOffset(latestOffsetValue, "");
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing Spark Connector");
        SolaceConnectionManager.close();
    }

}
