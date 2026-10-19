package com.solacecoe.connectors.spark.streaming;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.files.CreateDirectoryRequest;
import com.databricks.sdk.service.files.DownloadResponse;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
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
    private final boolean isDatabricks;
    private final boolean isUCVolume;
//    private boolean rotateSecret;
//    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;
    private Map<String, String> properties = new HashMap<>();
    private final SolaceBroker solaceBroker;
    private String queueName = "";
    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> currentCheckpoint = new CopyOnWriteArrayList<>();
    private final String checkpointLocation;
    private final List<String> partitionIds = new ArrayList<>();
    private WorkspaceClient workspaceClient;
    private ScheduledExecutorService databricksSecretRefresh;
    private ScheduledFuture<?> refreshTask;
    public SolaceMicroBatch(Map<String, String> properties, String checkpointLocation, boolean isDatabricks, boolean isUCVolume) {
        this.properties = new HashMap<>(properties);;

        this.isDatabricks = isDatabricks;
        this.isUCVolume = isUCVolume;

        SolaceUtils.validateCommonProperties(properties);

        log.info("SolaceSparkConnector - isDatabricks {} and isUCVolume {}", isDatabricks, isUCVolume);

        if(isDatabricks && isUCVolume) {
            if(properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE, "").isEmpty()) {
                throw new RuntimeException("SolaceSparkConnector - DATABRICKS_SECRET_SCOPE for Databricks Host, ClientId and Client Secret is required when using Unity Catalog Volumes as checkpoint location");
            }

            if(properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_HOST, "").isEmpty()) {
                throw new RuntimeException("SolaceSparkConnector - DATABRICKS_HOST property is required when using Unity Catalog Volumes as checkpoint location");
            }

//            rotateSecret = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_ROTATE_CLIENT_SECRET, SolaceSparkStreamingProperties.DATABRICKS_ROTATE_CLIENT_SECRET_DEFAULT));

//            if(rotateSecret) {
//                if(properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_SERVICE_PRINCIPAL_ID, "").isEmpty()) {
//                    throw new RuntimeException("SolaceSparkConnector - DATABRICKS_SERVICE_PRINCIPAL_ID is required when using Unity Catalog Volumes as checkpoint location and DATABRICKS_ROTATE_CLIENT_SECRET is set to true");
//                }
//
//                if(Long.parseLong(properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET_LIFETIME, "0")) <= 0) {
//                    throw new RuntimeException("SolaceSparkConnector - Invalid DATABRICKS_CLIENT_SECRET_LIFETIME is configured and DATABRICKS_ROTATE_CLIENT_SECRET is set to true. Value should be greater than zero");
//                }
//
//                String clientSecret = createSecret();
//                this.properties.put(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET, clientSecret);
//            } else {

            if(properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_ID, "").isEmpty()) {
                throw new RuntimeException("SolaceSparkConnector - DATABRICKS_CLIENT_ID is required when using Unity Catalog Volumes as checkpoint location");
            }

            if(properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET, "").isEmpty()) {
                throw new RuntimeException("SolaceSparkConnector - DATABRICKS_CLIENT_SECRET is required when using Unity Catalog Volumes as checkpoint location");
            }

            workspaceClient = new WorkspaceClient();

            DatabricksConfig databricksConfig = new DatabricksConfig();
            String host = workspaceClient.secrets().get(properties.get(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE), properties.get(SolaceSparkStreamingProperties.DATABRICKS_HOST));
            String clientId = workspaceClient.secrets().get(properties.get(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE), properties.get(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_ID));
            databricksConfig.setHost(host);
            databricksConfig.setClientId(clientId);
            databricksConfig.setClientSecret(workspaceClient.secrets().get(properties.get(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE), properties.get(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET)));
            // re-initialize with oauth-m2m as it is supported authentication mechanism.
            workspaceClient = new WorkspaceClient(databricksConfig);
            this.properties.put(SolaceSparkStreamingProperties.DATABRICKS_HOST + "_value", host);

            this.databricksSecretRefresh = Executors.newScheduledThreadPool(1);

            this.properties.put(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_ID + "_value", clientId);

            refreshCredentials();
            scheduleSecretRefresh();
//            }
        }

        this.checkpointLocation = convertCheckpointURIToStringPath(checkpointLocation);
        log.info("SolaceSparkConnector - Configured Checkpoint location {}", checkpointLocation);
//        this.checkpoints = new CopyOnWriteArrayList<>();
        log.info("SolaceSparkConnector - Initializing Solace Spark Connector");
        // Initialize classes required for Solace connectivity

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
        if(currentCheckpoint != null && currentCheckpoint.isEmpty()) {
            currentCheckpoint = this.getCheckpoint();
        }
        return new SolaceDataSourceReaderFactory(this.includeHeaders, isDatabricks, isUCVolume, this.properties, currentCheckpoint, this.checkpointLocation);
    }

    @Override
    public Offset initialOffset() {
        CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> existingCheckpoints = this.getCheckpoint();
        if(existingCheckpoints != null && !existingCheckpoints.isEmpty()) {
            currentCheckpoint = existingCheckpoints;
            log.info("SolaceSparkConnector - Checkpoint available from LVQ {}", new Gson().toJson(existingCheckpoints));
            return new SolaceSourceOffset(lastKnownOffsetId, existingCheckpoints);
        }
        log.info("SolaceSparkConnector - Initial Offset from LVQ is not available, the micro integration will use the available offset in checkpoint else a new checkpoint state will be created");
        return new SolaceSourceOffset(lastKnownOffsetId, new CopyOnWriteArrayList<>());
    }

    @Override
    public Offset deserializeOffset(String json) {
        SolaceSourceOffset solaceSourceOffset = getDeserializedOffset(json);
        if(solaceSourceOffset.getCheckpoints() != null && solaceSourceOffset.getCheckpoints().isEmpty()) {
            log.info("SolaceSparkConnector - No offset is available in spark checkpoint location. New checkpoint state will be created");
        } else {
            log.trace("SolaceSparkConnector - Deserialized offset {}", new Gson().toJson(solaceSourceOffset));
        }
        lastKnownOffsetId = solaceSourceOffset.getOffset();
        currentCheckpoint = solaceSourceOffset.getCheckpoints();

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
            } else {
                return solaceSourceOffset;
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
                throw new RuntimeException("SolaceSparkConnector - Exception when migrating offset to latest format. Please delete the checkpoint and restart the micro integration", e);
            }
        }
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
            if(this.isDatabricks && isUCVolume) {
                try {
                    DownloadResponse downloadResponse = workspaceClient.files().download(this.checkpointLocation + "/" + partitionId + ".txt");
                    try (Stream<String> lines = new BufferedReader(
                            new InputStreamReader(downloadResponse.getContents(), StandardCharsets.UTF_8)
                    ).lines()) {
                        offsetToCommit = updateOffset(lines, offsetToCommit);
                    }
                } catch (Exception e) {
                    // NOT_FOUND = file doesn't exist
                    if (e.getMessage().contains("NOT_FOUND")) {
                        log.warn("SolaceSparkConnector - File {} doesn't exist. Ignoring the error {}", this.checkpointLocation + "/" + partitionId + ".txt", e.getMessage());
                    } else {
                        throw e;
                    }
                }
            } else {
                Path path = Paths.get(this.checkpointLocation + "/" + partitionId + ".txt");
                if (Files.exists(path)) {
                    try (Stream<String> lines = Files.lines(path)) {
                        offsetToCommit = updateOffset(lines, offsetToCommit);
//                    for(String line: lines.collect(Collectors.toList())) {
//                        if (offsetToCommit.isEmpty()) {
//                            offsetToCommit = new Gson().fromJson(line, new TypeToken<CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint>>() {
//                            }.getType());
//                        } else {
//                            offsetToCommit.addAll(new Gson().fromJson(line, new TypeToken<CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint>>() {
//                            }.getType()));
//                            offsetToCommit = offsetToCommit.stream().distinct().collect(Collectors.toCollection(CopyOnWriteArrayList::new));
//                        }
//                    };
                    } catch (IOException e) {
                        log.error("SolaceSparkConnector - Exception when creating checkpoint to store in Solace LVQ", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        if(!offsetToCommit.isEmpty()) {
            currentCheckpoint = offsetToCommit;
            log.info("SolaceSparkConnector - Final checkpoint published to LVQ on topic {}", properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC));
            log.trace("SolaceSparkConnector - Final checkpoint publishing to LVQ {} on topic {}", new Gson().toJson(offsetToCommit), properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC));
            this.solaceBroker.publishMessage(properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC), new Gson().toJson(offsetToCommit));
            checkException();
            offsetToCommit.clear();
        }
    }

    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> updateOffset(Stream<String> lines, CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> offsetToCommit) {
        Gson gson = new Gson();
        lines.forEach(line -> {
            CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> parsed =
                    gson.fromJson(
                            line,
                            new TypeToken<CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint>>() {
                            }.getType()
                    );
            offsetToCommit.addAll(parsed);
        });

        return offsetToCommit.stream()
                .distinct()
                .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing Spark Connector");
        checkException();
        if (refreshTask != null) {
            refreshTask.cancel(false); // don't interrupt ongoing refresh
        }
        if(this.databricksSecretRefresh != null && !this.databricksSecretRefresh.isShutdown()) {
            this.databricksSecretRefresh.shutdown();
            try {
                if (!this.databricksSecretRefresh.awaitTermination(5, TimeUnit.SECONDS)) {
                    this.databricksSecretRefresh.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.databricksSecretRefresh.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
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
        if(this.isDatabricks && this.isUCVolume) {
            log.info("SolaceSparkConnector - Runtime platform is Databricks and Unity Catalog Volume is configured as checkpoint location");
            log.info("SolaceSparkConnector - Initializing Databricks SDK to connect to Unity Catalog Volume");

            checkpointLocation = checkpointLocation.replaceFirst("dbfs:/+", "/");

            CreateDirectoryRequest createDirectoryRequest = new CreateDirectoryRequest();
            createDirectoryRequest.setDirectoryPath(checkpointLocation);
            // Creates directory or returns success on existing directory
            workspaceClient.files().createDirectory(createDirectoryRequest);
        } else {
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
        }

        return checkpointLocation;
    }

    private void scheduleSecretRefresh() {
        // Schedule periodic refresh
        refreshTask = this.databricksSecretRefresh.scheduleWithFixedDelay(
                this::refreshCredentials,
                Long.parseLong(this.properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET_REFRESH_INTERVAL, "1440")),
                Long.parseLong(this.properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET_REFRESH_INTERVAL, "1440")),
                TimeUnit.MINUTES
        );
        log.info("SolaceSparkConnector - Scheduled Databricks Client Secret refresh from secret scope {} with name {} for every {} minutes",
                properties.get(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE),
                properties.get(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET),
                properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET_REFRESH_INTERVAL, "1440"));
    }

    private void refreshCredentials() {
        try {
            String clientSecret = workspaceClient.secrets().get(properties.get(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE), properties.get(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET));
            this.properties.put(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET + "_value", clientSecret);
            log.info("SolaceSparkConnector - Refreshed Databricks client secret from secret scope {} with name {}", properties.get(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE), properties.get(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET));
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Failed to refresh Databricks client secret " +
                            "from secret scope '{}'. Retaining existing secret until next refresh cycle in {} minutes." +
                            "If the existing secret expires before the next refresh cycle, " +
                            "the connector will fail on the next Databricks Unity Catalog Volume access. ",
                    properties.get(SolaceSparkStreamingProperties.DATABRICKS_SECRET_SCOPE),
                    properties.getOrDefault(SolaceSparkStreamingProperties.DATABRICKS_CLIENT_SECRET_REFRESH_INTERVAL, "1440"), e);
        }
    }
}