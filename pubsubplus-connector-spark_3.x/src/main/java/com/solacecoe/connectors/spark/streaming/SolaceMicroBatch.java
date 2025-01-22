package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSourceOffset;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceMicroBatch implements MicroBatchStream {
    private static final Logger log = LogManager.getLogger(SolaceMicroBatch.class);
    private int lastKnownOffsetId = 0;
    private int latestOffsetId = 0;
    private int initialOffsetCount = 0;
    private final SolaceInputPartition[] inputPartitions;
    private final Map<String, SolaceInputPartition> inputPartitionsList = new HashMap<>();
    private final int partitions;
    private final int batchSize;
    private final boolean includeHeaders;
    private boolean initialOffset = false;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;
    private String lastKnownMessageIds = "";
    private boolean isCommitTriggered = false;

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

        this.solaceBroker = new SolaceBroker(properties, "lvq-consumer");
        LVQEventListener lvqEventListener = new LVQEventListener();
        this.solaceBroker.addLVQReceiver(lvqEventListener);
        SolaceConnectionManager.addConnection("lvq-"+0, this.solaceBroker);
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        latestOffsetId+=batchSize;
        return new SolaceSourceOffset(latestOffsetId, new CopyOnWriteArrayList<>());
    }

//    @Override
//    public Offset latestOffset(Offset startOffset, ReadLimit limit) {
////        JsonObject jsonObject = this.solaceBroker.getOffsetFromLvq();
////        jsonObject.entrySet().forEach((entry) -> {
////            SolaceSparkOffset solaceSparkOffset = new Gson().fromJson(entry.getValue().getAsString(), SolaceSparkOffset.class);
////            solaceSparkOffset.setStartOffset(solaceSparkOffset.getEndOffset());
////            solaceSparkOffset.setEndOffset(solaceSparkOffset.getEndOffset() + batchSize);
////            solaceSparkOffset.setMessageIDs(solaceSparkOffset.getMessageIDs());
////
////            jsonObject.addProperty(entry.getKey(), solaceSparkOffset.json());
////        });
////        if(!jsonObject.isEmpty()) {
////            Map<String, SolaceSparkOffset> solaceSparkOffsets = new HashMap<>();
////            jsonObject.entrySet().forEach((entry) -> {
////                SolaceSparkOffset solaceSparkOffset = new Gson().fromJson(entry.getValue().getAsString(), SolaceSparkOffset.class);
//////                solaceSparkOffset.setStartOffset(solaceSparkOffset.getEndOffset());
//////                solaceSparkOffset.setEndOffset(solaceSparkOffset.getEndOffset() + batchSize);
//////                solaceSparkOffset.setMessageIDs(solaceSparkOffset.getMessageIDs());
////                solaceSparkOffsets.put(entry.getKey(), solaceSparkOffset);
////            });
////            return new SolaceSourceOffset(solaceSparkOffsets);
////        } else {
////            JsonObject offsets = new Gson().fromJson(startOffset.json(), JsonObject.class);
////            Map<String, SolaceSparkOffset> solaceSparkOffsets = new HashMap<>();
////            offsets.entrySet().forEach((entry) -> {
////                solaceSparkOffsets.put(entry.getKey(), new Gson().fromJson(entry.getValue(), SolaceSparkOffset.class));
////            });
////            SolaceSourceOffset sourceOffset = new SolaceSourceOffset(solaceSparkOffsets);
////            sourceOffset.getOffsets().forEach((key, value) -> {
////                value.setStartOffset(value.getEndOffset());
////                value.setEndOffset(value.getEndOffset() + batchSize);
//////                value.setMessageIDs("");
////
////                sourceOffset.getOffsets().put(key, value);
////            });
////
////            return sourceOffset;
////        }
//        latestOffsetId+=batchSize;
//        TempOffset tempOffset = new TempOffset();
//        tempOffset.setOffset(latestOffsetId);
//
//        return tempOffset;
//    }

//    private InputPartition[] getPartitions() {
////        for(int i=0; i < this.partitions; i++) {
//////            /solaceSparkOffsetManager.messageIDs = offsetJson != null && offsetJson.has("messageIDs") ? Arrays.asList(offsetJson.get("messageIDs").getAsString().split(",")) : new ArrayList<>();
////            inputPartitions[i] = new SolaceInputPartition("partition-" + i, latestOffsetId, "");
////        }
//        inputPartitionsList.add(new SolaceInputPartition("partition-0", latestOffsetId, ""));
//        return inputPartitionsList.toArray(new InputPartition[0]);
//    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
//        System.out.println("Start offset " + start.json());
//        System.out.println("End offset " + end.json());
//        JsonObject jsonObject = new Gson().fromJson(start.json(), JsonObject.class);
//        final int[] i = {0};
//        jsonObject.keySet().forEach((key) -> {
////            if(inputPartitionsList.isEmpty() || inputPartitionsList.get(i[0]) == null || inputPartitionsList.size() < 2) {
//                inputPartitionsList.add(new SolaceInputPartition(key + UUID.randomUUID(), latestOffsetId, ""));
////            }
//            i[0]++;
//        });

        for(int i=0; i<partitions; i++) {
            inputPartitionsList.put("partition-" + i, new SolaceInputPartition("partition-" + i, latestOffsetId, ""));
        }

        return inputPartitionsList.values().toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.info("SolaceSparkConnector - Create reader factory with includeHeaders :: {}", this.includeHeaders);
        return new SolaceDataSourceReaderFactory(this.includeHeaders, this.lastKnownMessageIds, this.properties);
    }

    @Override
    public Offset initialOffset() {
//        initialOffsetCount++;
//        JsonObject jsonObject = this.solaceBroker.getOffsetFromLvq();
//        if(jsonObject != null && !jsonObject.isEmpty() && this.initialOffsetCount < 2) {
//            lastKnownOffset = jsonObject;
////            System.out.println("Existing Initial offset " + jsonObject);
//            Map<String, SolaceSparkOffset> solaceSparkOffsets = new HashMap<>();
//            jsonObject.entrySet().forEach((entry) -> {
//                solaceSparkOffsets.put(entry.getKey(), new Gson().fromJson(entry.getValue(), SolaceSparkOffset.class));
//            });
//
//            return new SolaceSourceOffset(solaceSparkOffsets);
//        }
//
//        this.initialOffset = true;
//        Map<String, SolaceSparkOffset> offsets = new HashMap<>();
//        offsets.put("partition-0", new SolaceSparkOffset(0, 0 , ""));
//        SolaceSourceOffset sourceOffset = new SolaceSourceOffset(offsets);
////        System.out.println("Initial offset " + sourceOffset.json());
//
//        return sourceOffset;

        CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints = this.solaceBroker.getOffsetFromLvq();
        if(checkpoints != null && !checkpoints.isEmpty()) {
            checkpoints.forEach(checkpoint -> {
                lastKnownMessageIds = String.join(",", lastKnownMessageIds, checkpoint.getMessageIDs());
            });

            return new SolaceSourceOffset(lastKnownOffsetId, checkpoints);
        }
        return new SolaceSourceOffset(lastKnownOffsetId, new CopyOnWriteArrayList<>());
    }

    @Override
    public Offset deserializeOffset(String json) {
        SolaceSourceOffset solaceSourceOffset = new Gson().fromJson(json, SolaceSourceOffset.class);
        if(solaceSourceOffset != null) {
            lastKnownOffsetId = solaceSourceOffset.getOffset();
        }
//        System.out.println("Deserializing offset json: " + json);
//        JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
//        Map<String, SolaceSparkOffset> solaceSparkOffsets = new HashMap<>();
//        jsonObject.entrySet().forEach((entry) -> {
//            solaceSparkOffsets.put(entry.getKey(), new Gson().fromJson(entry.getValue(), SolaceSparkOffset.class));
//        });
//        return new SolaceSourceOffset(solaceSparkOffsets);

        return solaceSourceOffset;
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
//        System.out.println("SolaceSparkConnector - Commit triggered " + end.json());
//        latestOffsetId += (lastKnownOffsetId + batchSize);
        isCommitTriggered = true;
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing Spark Connector");
        SolaceConnectionManager.close();
    }

}