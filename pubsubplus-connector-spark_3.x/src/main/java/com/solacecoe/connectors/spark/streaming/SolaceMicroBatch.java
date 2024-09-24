package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkOffset;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceDataSourceReaderFactory;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceInputPartition;
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

public class SolaceMicroBatch implements MicroBatchStream, SupportsAdmissionControl {
    private static final Logger log = LogManager.getLogger(SolaceMicroBatch.class);
    private int lastKnownOffsetId = 0;
    private final SolaceInputPartition[] inputPartitions;
    private final int partitions;
    private final int batchSize;
    private final boolean includeHeaders;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;
    private JsonObject lastKnownOffset = new JsonObject();

    public SolaceMicroBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.properties = properties;
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

        if(!properties.containsKey(SolaceSparkStreamingProperties.USERNAME) || properties.get(SolaceSparkStreamingProperties.USERNAME) == null || properties.get(SolaceSparkStreamingProperties.USERNAME).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Username in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Username in configuration options");
        }

        if(!properties.containsKey(SolaceSparkStreamingProperties.PASSWORD) || properties.get(SolaceSparkStreamingProperties.PASSWORD) == null || properties.get(SolaceSparkStreamingProperties.PASSWORD).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Password in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Password in configuration options");
        }

        if(!properties.containsKey(SolaceSparkStreamingProperties.QUEUE) || properties.get(SolaceSparkStreamingProperties.QUEUE) == null || properties.get(SolaceSparkStreamingProperties.QUEUE).isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }

        this.batchSize = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT));
        includeHeaders = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        log.info("SolaceSparkConnector - includeHeaders is set to {}", includeHeaders);

        partitions = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.PARTITIONS, SolaceSparkStreamingProperties.PARTITIONS_DEFAULT));
        log.info("SolaceSparkConnector - Partitions is set to {}", partitions);
        inputPartitions = new SolaceInputPartition[partitions];

        String solaceOffsetIndicator = properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT);
        log.info("SolaceSparkConnector - offsetIndicator is set to {}", solaceOffsetIndicator);

        this.solaceBroker = new SolaceBroker(properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME), properties.get(SolaceSparkStreamingProperties.PASSWORD), properties.get(SolaceSparkStreamingProperties.QUEUE), properties);
        LVQEventListener lvqEventListener = new LVQEventListener();
        this.solaceBroker.addLVQReceiver(lvqEventListener);
        SolaceConnectionManager.addConnection("lvq-"+0, this.solaceBroker);
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        lastKnownOffsetId+=batchSize;
//        log.info("SolaceSparkConnector - latestOffset :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        SolaceSparkOffset solaceSparkOffset = this.solaceBroker.getLVQMessage();
        if(solaceSparkOffset != null) {
            return new SolaceSparkOffset(lastKnownOffsetId, solaceSparkOffset.getQueryId(),
                    solaceSparkOffset.getBatchId(), solaceSparkOffset.getStageId(),
                    solaceSparkOffset.getPartitionId(), solaceSparkOffset.getMessageIDs());
        }
//        log.info("SolaceSparkConnector - latestOffset with params :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return getDefaultOffset();
//        return new SolaceSparkOffset(lastKnownOffsetId, "");
    }

    private InputPartition[] getPartitions() {
        for(int i=0; i < this.partitions; i++) {
//            /solaceSparkOffsetManager.messageIDs = offsetJson != null && offsetJson.has("messageIDs") ? Arrays.asList(offsetJson.get("messageIDs").getAsString().split(",")) : new ArrayList<>();
            inputPartitions[i] = new SolaceInputPartition("partition-"+i, lastKnownOffsetId, "");
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
        return new SolaceDataSourceReaderFactory(this.includeHeaders, this.lastKnownOffset.toString(), this.properties);
    }

    @Override
    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
        lastKnownOffsetId+=batchSize;
        SolaceSparkOffset solaceSparkOffset = this.solaceBroker.getLVQMessage();
        if(solaceSparkOffset != null) {
            return new SolaceSparkOffset(lastKnownOffsetId, solaceSparkOffset.getQueryId(),
                    solaceSparkOffset.getBatchId(), solaceSparkOffset.getStageId(),
                    solaceSparkOffset.getPartitionId(), solaceSparkOffset.getMessageIDs());
        }
//        log.info("SolaceSparkConnector - latestOffset with params :: (key,value) - (" + latestOffsetValue + "," + String.join(",", this.processedMessageIDs) + ")");
        return getDefaultOffset();
    }

    @Override
    public Offset initialOffset() {
        SolaceSparkOffset solaceSparkOffset = this.solaceBroker.getLVQMessage();
        if(solaceSparkOffset != null) {
            return new SolaceSparkOffset(lastKnownOffsetId, solaceSparkOffset.getQueryId(),
                    solaceSparkOffset.getBatchId(), solaceSparkOffset.getStageId(),
                    solaceSparkOffset.getPartitionId(), solaceSparkOffset.getMessageIDs());
        }

        return getDefaultOffset();
    }

    @Override
    public Offset deserializeOffset(String json) {
        lastKnownOffset = new Gson().fromJson(json, JsonObject.class);
        if(lastKnownOffset != null && lastKnownOffset.has("offset")) {
            lastKnownOffsetId = lastKnownOffset.get("offset").getAsInt();
        }

        SolaceSparkOffset solaceSparkOffset = this.solaceBroker.getLVQMessage();
        if(solaceSparkOffset != null) {
            return new SolaceSparkOffset(solaceSparkOffset.getOffset(), solaceSparkOffset.getQueryId(),
                    solaceSparkOffset.getBatchId(), solaceSparkOffset.getStageId(),
                    solaceSparkOffset.getPartitionId(), solaceSparkOffset.getMessageIDs());
        }
        return getDefaultOffset();
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

    private SolaceSparkOffset getDefaultOffset() {
        String queryId = "";
        String batchId = "";
        String stageId = "";
        String partitionId = "";
        String messageIds = "";
        if(lastKnownOffset != null) {
            if(lastKnownOffset.has("queryId")) {
                queryId = lastKnownOffset.get("queryId").getAsString();
            }
            if(lastKnownOffset.has("batchId")) {
                batchId = lastKnownOffset.get("batchId").getAsString();
            }
            if(lastKnownOffset.has("stageId")) {
                stageId = lastKnownOffset.get("stageId").getAsString();
            }
            if(lastKnownOffset.has("partitionId")) {
                partitionId = lastKnownOffset.get("partitionId").getAsString();
            }
            if(lastKnownOffset.has("messageIDs")) {
                messageIds = lastKnownOffset.get("messageIDs").getAsString();
            }
        }
        return new SolaceSparkOffset(lastKnownOffsetId, queryId, batchId, stageId, partitionId, messageIds);
    }

}
