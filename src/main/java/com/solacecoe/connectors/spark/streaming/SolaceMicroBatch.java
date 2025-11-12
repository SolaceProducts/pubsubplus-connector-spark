package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSourceOffset;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceDataSourceReaderFactory;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceInputPartition;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidPropertyException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.ExecutorCacheTaskLocation;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.BlockManagerMaster;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolaceMicroBatch implements MicroBatchStream {
    private static final Logger log = LogManager.getLogger(SolaceMicroBatch.class);
    private int lastKnownOffsetId = 0;
    private int latestOffsetId = 0;
    private final Map<String, SolaceInputPartition> inputPartitionsList = new HashMap<>();
    private int partitions;
    private final int batchSize;
    private final boolean includeHeaders;
//    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;
    private String lastKnownMessageIds = "";
    private String queueName = "";
    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> currentCheckpoint = new CopyOnWriteArrayList<>();
    private final String checkpointLocation;
    private final List<String> partitionIds = new ArrayList<>();
    public SolaceMicroBatch(Map<String, String> properties, String checkpointLocation) {
        this.properties = properties;

        this.checkpointLocation = convertCheckpointURIToStringPath(checkpointLocation);
        log.info("SolaceSparkConnector - Configured Checkpoint location {}", checkpointLocation);
//        this.checkpoints = new CopyOnWriteArrayList<>();
        log.info("SolaceSparkConnector - Initializing Solace Spark Connector");
        // Initialize classes required for Solace connectivity

        SolaceUtils.validateCommonProperties(properties);

        if(!properties.containsKey(SolaceSparkStreamingProperties.QUEUE) || properties.get(SolaceSparkStreamingProperties.QUEUE) == null || properties.get(SolaceSparkStreamingProperties.QUEUE).isEmpty()) {
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }

        this.batchSize = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.BATCH_SIZE, SolaceSparkStreamingProperties.BATCH_SIZE_DEFAULT));
        latestOffsetId = (-batchSize);
        if(this.batchSize < 0) {
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please set batch size greater than zero");
        }
        includeHeaders = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        log.info("SolaceSparkConnector - includeHeaders is set to {}", includeHeaders);

        partitions = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.PARTITIONS, SolaceSparkStreamingProperties.PARTITIONS_DEFAULT));
        log.info("SolaceSparkConnector - Partitions is set to {}", partitions);

        String solaceOffsetIndicator = properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT);
        log.info("SolaceSparkConnector - offsetIndicator is set to {}", solaceOffsetIndicator);

        this.queueName = properties.getOrDefault(SolaceSparkStreamingProperties.QUEUE, "");
        this.solaceBroker = new SolaceBroker(properties, "monitoring-consumer");
//        LVQEventListener lvqEventListener = new LVQEventListener();
//        this.solaceBroker.addLVQReceiver(lvqEventListener);
        this.solaceBroker.createLVQIfNotExist();
        this.solaceBroker.initProducer();
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        currentCheckpoint = this.getCheckpoint();
        checkException();
        if(!this.solaceBroker.isQueueFull()) {
            checkException();
            log.info("SolaceSparkConnector - Queue {} is empty. Skipping batch", queueName);
            return new SolaceSourceOffset(latestOffsetId, currentCheckpoint);
        }
        checkException();
        latestOffsetId+=batchSize;
        if(currentCheckpoint != null && !currentCheckpoint.isEmpty()) {
            currentCheckpoint.forEach(checkpoint -> lastKnownMessageIds = String.join(",", lastKnownMessageIds, checkpoint.getMessageIDs()));
            return new SolaceSourceOffset(latestOffsetId, currentCheckpoint);
        } else {
            currentCheckpoint = new CopyOnWriteArrayList<>();
        }
        return new SolaceSourceOffset(latestOffsetId, new CopyOnWriteArrayList<>());
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        checkException();
        if(partitions == 0) {
            partitions = getTotalExecutors();
        }
        for (int i = 0; i < partitions; i++) {
            int partitionHashCode = (queueName + "-" + i).hashCode();
            if(!partitionIds.contains(Integer.toString(partitionHashCode))) {
                partitionIds.add(Integer.toString(partitionHashCode));
            }
            Optional<String> preferredLocation = getExecutorLocation(getSortedExecutorList(), partitionHashCode);
            inputPartitionsList.put(String.valueOf(partitionHashCode), new SolaceInputPartition(partitionHashCode, preferredLocation.orElse("")));
        }

        return inputPartitionsList.values().toArray(new InputPartition[0]);
    }

    private int getTotalExecutors() {
        return getExecutorList().size();
    }

    private List<ExecutorCacheTaskLocation> getExecutorList() {
        BlockManager bm = SparkEnv.get().blockManager();
        BlockManagerMaster master = bm.master();

        // Get the list of peers (executors)
        Seq<BlockManagerId> peersSeq = master.getPeers(bm.blockManagerId());

        // Convert Scala Seq to a Java List
        List<BlockManagerId> peers = JavaConverters.seqAsJavaList(peersSeq);

        List<ExecutorCacheTaskLocation> executorList = new ArrayList<>();

        // Convert BlockManagerId to ExecutorCacheTaskLocation
        for (BlockManagerId x : peers) {
            executorList.add(new ExecutorCacheTaskLocation(x.host(), x.executorId()));
        }

        return executorList;
    }

    private List<String> getSortedExecutorList() {
        List<ExecutorCacheTaskLocation> executorList = getExecutorList();

        log.info("SolaceSparkConnector - Available executor nodes {}", executorList.size());

        // Sort the list based on the compare logic
        executorList.sort((a, b) -> {
            if (a.host().equals(b.host())) {
                return a.executorId().compareTo(b.executorId());
            } else {
                return a.host().compareTo(b.host());
            }
        });

        // Map the result to string and return
        return executorList.stream().map(ExecutorCacheTaskLocation::toString).collect(Collectors.toList());
    }

    // Equivalent of floorMod function
    private int floorMod(long a, int b) {
        return (int)((a % b + b) % b);
    }

    private Optional<String> getExecutorLocation(List<String> executorLocations, int partitionHashCode) {
        int numExecutors = executorLocations.size();

        if (numExecutors > 0) {
            int executorIndex = floorMod(partitionHashCode, numExecutors);
            String executor = executorLocations.get(executorIndex);
            log.info("SolaceSparkConnector - Preferred location for partition {} is at executor {}", partitionHashCode, executor);
            return Optional.of(executorLocations.get(executorIndex));
        } else {
            log.info("SolaceSparkConnector - No Executors present");
            return Optional.empty();
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.info("SolaceSparkConnector - Create reader factory with includeHeaders :: {}", this.includeHeaders);
        return new SolaceDataSourceReaderFactory(this.includeHeaders, this.properties, currentCheckpoint, this.checkpointLocation);
    }

    @Override
    public Offset initialOffset() {
        CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> existingCheckpoints = this.getCheckpoint();
        if(existingCheckpoints != null && !existingCheckpoints.isEmpty()) {
            currentCheckpoint = existingCheckpoints;
            existingCheckpoints.forEach(checkpoint -> lastKnownMessageIds = String.join(",", lastKnownMessageIds, checkpoint.getMessageIDs()));

            return new SolaceSourceOffset(lastKnownOffsetId, existingCheckpoints);
        }

        return new SolaceSourceOffset(lastKnownOffsetId, new CopyOnWriteArrayList<>());
    }

    @Override
    public Offset deserializeOffset(String json) {
        SolaceSourceOffset solaceSourceOffset = getDeserializedOffset(json);
        if(solaceSourceOffset != null) {
            lastKnownOffsetId = solaceSourceOffset.getOffset();
            solaceSourceOffset.getCheckpoints().forEach(checkpoint -> lastKnownMessageIds = String.join(",", lastKnownMessageIds, checkpoint.getMessageIDs()));
        }

        return solaceSourceOffset;
    }

    private SolaceSourceOffset getDeserializedOffset(String json) {
        try {
            SolaceSourceOffset solaceSourceOffset = new Gson().fromJson(json, SolaceSourceOffset.class);
            if(solaceSourceOffset.getCheckpoints() == null) {
                JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
                if (jsonObject.has("messageIDs")) {
                    return migrate(solaceSourceOffset.getOffset(), jsonObject.get("messageIDs").getAsString());
                } else {
                    return migrate(solaceSourceOffset.getOffset(), "");
                }
            }
        } catch (Exception e) {
            log.warn("SolaceSparkConnector - Exception when deserializing offset. May be due incompatible formats. Connector will try to migrate to latest offset format.");
            try {
                JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
                if (jsonObject.has("messageIDs")) {
                    return migrate(jsonObject.get("offset").getAsInt(), jsonObject.get("messageIDs").getAsString());
                } else {
                    return migrate(jsonObject.get("offset").getAsInt(), "");
                }
            } catch (Exception e2) {
                log.error("SolaceSparkConnector - Exception when migrating offset to latest format.");
                throw new RuntimeException("SolaceSparkConnector - Exception when migrating offset to latest format.", e);
            }
        }

        return null;
    }

    private SolaceSourceOffset migrate(int offset, String messageIds) {
        SolaceSparkPartitionCheckpoint solaceSparkPartitionCheckpoint = new SolaceSparkPartitionCheckpoint(messageIds, "old-checkpoint");
        CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints = new CopyOnWriteArrayList<>();
        checkpoints.add(solaceSparkPartitionCheckpoint);
        return new SolaceSourceOffset(offset, checkpoints);
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
        CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> offsetToCommit = new CopyOnWriteArrayList<>();
        for(String partitionId: partitionIds) {
            Path path = Paths.get(this.checkpointLocation + "/" + partitionId + ".txt");
            if(Files.exists(path)) {
                try (Stream<String> lines = Files.lines(path)) {
                    for(String line: lines.collect(Collectors.toList())) {
                        if (offsetToCommit.isEmpty()) {
                            offsetToCommit = new Gson().fromJson(line, new TypeToken<CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint>>() {
                            }.getType());
                        } else {
                            offsetToCommit.addAll(new Gson().fromJson(line, new TypeToken<CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint>>() {
                            }.getType()));
                            offsetToCommit = offsetToCommit.stream().distinct().collect(Collectors.toCollection(CopyOnWriteArrayList::new));
                        }
                    };
                } catch (IOException e) {
                    log.error("SolaceSparkConnector - Exception when creating checkpoint to store in Solace LVQ", e);
                    throw new RuntimeException(e);
                }
            }
        }

        if(!offsetToCommit.isEmpty()) {
            currentCheckpoint = offsetToCommit;
            log.trace("SolaceSparkConnector - Final checkpoint publishing to LVQ {}", new Gson().toJson(offsetToCommit));
            this.solaceBroker.publishMessage(properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC), new Gson().toJson(offsetToCommit));
            checkException();
            offsetToCommit.clear();
        }
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing Spark Connector");
        checkException();
        this.solaceBroker.close();
    }

    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> getCheckpoint() {
//        return this.solaceBroker.getOffsetFromLvq();
        return this.solaceBroker.browseLVQ();
    }

    private void checkException() {
        if(this.solaceBroker.isException()) {
            this.solaceBroker.shutdownExecutor();
            throw new RuntimeException(this.solaceBroker.getException());
        }
    }

    private String convertCheckpointURIToStringPath(String checkpointLocation) {
        if (checkpointLocation.startsWith("dbfs:/")) {
            // Strip "dbfs:/" and prepend "/dbfs/"
            String dbfsPath = checkpointLocation.replaceFirst("dbfs:/+", "");
            checkpointLocation = String.valueOf(Paths.get("/dbfs", dbfsPath));
        } else if (checkpointLocation.startsWith("file:/")) {
            // Parse as standard URI
            URI fileUri = null;
            try {
                fileUri = new URI(checkpointLocation);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            checkpointLocation = String.valueOf(Paths.get(fileUri));
        }

        return checkpointLocation;
    }

}