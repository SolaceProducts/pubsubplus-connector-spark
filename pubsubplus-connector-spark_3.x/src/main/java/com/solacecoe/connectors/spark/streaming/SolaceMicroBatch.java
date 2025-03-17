package com.solacecoe.connectors.spark.streaming;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSourceOffset;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceDataSourceReaderFactory;
import com.solacecoe.connectors.spark.streaming.partitions.SolaceInputPartition;
import com.solacecoe.connectors.spark.streaming.solace.LVQEventListener;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidPropertyException;
import com.solacesystems.jcsmp.JCSMPProperties;
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

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class SolaceMicroBatch implements MicroBatchStream {
    private static final Logger log = LogManager.getLogger(SolaceMicroBatch.class);
    private int lastKnownOffsetId = 0;
    private int latestOffsetId = 0;
    private final Map<String, SolaceInputPartition> inputPartitionsList = new HashMap<>();
    private final int partitions;
    private final int batchSize;
    private final boolean includeHeaders;
    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;
    private final Map<String, String> properties;
    private final SolaceBroker solaceBroker;
    private String lastKnownMessageIds = "";
    private String queueName = "";
//    private final SolaceSourceInitialOffset solaceSourceInitialOffset;

    public SolaceMicroBatch(Map<String, String> properties, String checkpointLocation) {
        this.properties = properties;
        this.checkpoints = new CopyOnWriteArrayList<>();
//        solaceSourceInitialOffset = new SolaceSourceInitialOffset(SparkSession.getActiveSession().get(), checkpointLocation, scala.reflect.ClassTag$.MODULE$.apply(SolaceSourceOffset.class));
        log.info("SolaceSparkConnector - Initializing Solace Spark Connector");
        // Initialize classes required for Solace connectivity

        // User configuration validation
        if(!properties.containsKey(SolaceSparkStreamingProperties.HOST) || properties.get(SolaceSparkStreamingProperties.HOST) == null || properties.get(SolaceSparkStreamingProperties.HOST).isEmpty()) {
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(!properties.containsKey(SolaceSparkStreamingProperties.VPN) || properties.get(SolaceSparkStreamingProperties.VPN) == null || properties.get(SolaceSparkStreamingProperties.VPN).isEmpty()) {
            throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME) &&
                properties.get(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+ JCSMPProperties.AUTHENTICATION_SCHEME).equals(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)) {
            if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN)) {
                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL).isEmpty()) {
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client Authentication Server URL");
                }

                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID).isEmpty()) {
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client ID");
                }

                if(!properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET) == null || properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET).isEmpty()) {
                    throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client Credentials Secret");
                }

                String clientCertificate = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, null);
                if(clientCertificate != null) {
                    String trustStoreFilePassword = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, null);
                    if (trustStoreFilePassword == null || trustStoreFilePassword.isEmpty()) {
                        throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide OAuth Client TrustStore Password. If TrustStore file path is not configured, please provide password for default java truststore");
                    }
                }
            } else if(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, null) == null || properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN, null).isEmpty()) {
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide valid access token input");
            }
        } else {
            if (!properties.containsKey(SolaceSparkStreamingProperties.USERNAME) || properties.get(SolaceSparkStreamingProperties.USERNAME) == null || properties.get(SolaceSparkStreamingProperties.USERNAME).isEmpty()) {
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Username in configuration options");
            }

            if (!properties.containsKey(SolaceSparkStreamingProperties.PASSWORD) || properties.get(SolaceSparkStreamingProperties.PASSWORD) == null || properties.get(SolaceSparkStreamingProperties.PASSWORD).isEmpty()) {
                throw new SolaceInvalidPropertyException("SolaceSparkConnector - Please provide Solace Password in configuration options");
            }
        }

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
        this.solaceBroker = new SolaceBroker(properties, "lvq-consumer");
        LVQEventListener lvqEventListener = new LVQEventListener();
        this.solaceBroker.addLVQReceiver(lvqEventListener);
        log.info("SolaceSparkConnector - Initialization Completed");
    }

    @Override
    public Offset latestOffset() {
        latestOffsetId+=batchSize;
        checkpoints = this.getCheckpoint();
        if(checkpoints != null && !checkpoints.isEmpty()) {
            checkpoints.forEach(checkpoint -> lastKnownMessageIds = String.join(",", lastKnownMessageIds, checkpoint.getMessageIDs()));

            return new SolaceSourceOffset(latestOffsetId, checkpoints);
        } else {
            checkpoints = new CopyOnWriteArrayList<>();
        }
        return new SolaceSourceOffset(latestOffsetId, new CopyOnWriteArrayList<>());
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
//        if(inputPartitionsList.size() < partitions) {
        for (int i = 0; i < partitions; i++) {
            int partitionHashCode = (queueName + "-" + i).hashCode();
            Optional<String> preferredLocation = getExecutorLocation(getSortedExecutorList(), partitionHashCode);
            inputPartitionsList.put(String.valueOf(partitionHashCode), new SolaceInputPartition(partitionHashCode, latestOffsetId, preferredLocation.orElse("")));
        }
//        }

        return inputPartitionsList.values().toArray(new InputPartition[0]);
    }

    private List<String> getSortedExecutorList() {
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
            log.info("SolaceSparkConnector - Preferred location for partition {} is at executor {}", partitionHashCode, executorLocations.get(executorIndex));
            return Optional.of(executorLocations.get(executorIndex));
        } else {
            log.info("SolaceSparkConnector - No Executors present");
            return Optional.empty();
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        log.info("SolaceSparkConnector - Create reader factory with includeHeaders :: {}", this.includeHeaders);
        return new SolaceDataSourceReaderFactory(this.includeHeaders, this.lastKnownMessageIds, this.properties, this.checkpoints);
    }

    @Override
    public Offset initialOffset() {
        CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> existingCheckpoints = this.getCheckpoint();
        if(existingCheckpoints != null && !existingCheckpoints.isEmpty()) {
            existingCheckpoints.forEach(checkpoint -> lastKnownMessageIds = String.join(",", lastKnownMessageIds, checkpoint.getMessageIDs()));

            return new SolaceSourceOffset(lastKnownOffsetId, existingCheckpoints);
        }
//        log.info("SolaceSparkConnector - Getting initial offset from checkpoint location");
//        long batchId = (long) solaceSourceInitialOffset.getLatestBatchId().getOrElse(() -> 0l);
//        SolaceSourceOffset sourceOffset = (SolaceSourceOffset) solaceSourceInitialOffset.get(batchId).getOrElse(() -> new SolaceSourceOffset(lastKnownOffsetId, new CopyOnWriteArrayList<>()));
//        log.info("SolaceSparkConnector - initialOffset is set to {}", sourceOffset.json());
//        return (SolaceSourceOffset) solaceSourceInitialOffset.get(batchId).getOrElse(() -> new SolaceSourceOffset(lastKnownOffsetId, new CopyOnWriteArrayList<>()));
        return new SolaceSourceOffset(lastKnownOffsetId, new CopyOnWriteArrayList<>());
    }

    @Override
    public Offset deserializeOffset(String json) {
        SolaceSourceOffset solaceSourceOffset = new Gson().fromJson(json, SolaceSourceOffset.class);
        if(solaceSourceOffset != null) {
            lastKnownOffsetId = solaceSourceOffset.getOffset();
            solaceSourceOffset.getCheckpoints().forEach(checkpoint -> lastKnownMessageIds = String.join(",", lastKnownMessageIds, checkpoint.getMessageIDs()));
        }

        return solaceSourceOffset;
    }

    @Override
    public void commit(Offset end) {
        log.info("SolaceSparkConnector - Commit triggered");
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing Spark Connector");
        this.solaceBroker.close();
    }

    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> getCheckpoint() {
        return this.solaceBroker.getOffsetFromLvq();
    }

}