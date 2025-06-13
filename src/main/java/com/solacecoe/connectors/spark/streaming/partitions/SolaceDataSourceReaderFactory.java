package com.solacecoe.connectors.spark.streaming.partitions;

import com.solacecoe.connectors.spark.streaming.offset.SolaceMessageTracker;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.EventListener;
import com.solacecoe.connectors.spark.streaming.solace.SolaceBroker;
import com.solacecoe.connectors.spark.streaming.solace.SolaceConnectionManager;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceConsumerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.streaming.MicroBatchExecution;
import org.apache.spark.sql.execution.streaming.StreamExecution;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceDataSourceReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LogManager.getLogger(SolaceDataSourceReaderFactory.class);
    private final boolean includeHeaders;
    private final Map<String, String> properties;
    private final String checkpointLocation;
    private final CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;
    public SolaceDataSourceReaderFactory(boolean includeHeaders, Map<String, String> properties, CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints, String checkpointLocation) {
        this.includeHeaders = includeHeaders;
        this.properties = properties;
        this.checkpoints = checkpoints;
        this.checkpointLocation = checkpointLocation;
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            boolean ackLastProcessedMessages = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES, SolaceSparkStreamingProperties.ACK_LAST_PROCESSED_MESSAGES_DEFAULT));
            String replayStrategy = this.properties.getOrDefault(SolaceSparkStreamingProperties.REPLAY_STRATEGY ,null);
            if(replayStrategy == null || replayStrategy.isEmpty()) {
                ackLastProcessedMessages = false;
            }

            TaskContext taskCtx = TaskContext.get();
            String queryId = taskCtx.getLocalProperty(StreamExecution.QUERY_ID_KEY());
            String batchId = taskCtx.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY());
            SolaceInputPartition solaceInputPartition = (SolaceInputPartition) partition;
            // Check if connection manager already has existing connection but partition id is new to executor.
            // Required when executor is lost and got task got re-assigned to new executor
            if(SolaceConnectionManager.getPartition(solaceInputPartition.getId()) != null && !SolaceConnectionManager.getPartition(solaceInputPartition.getId()).equals(solaceInputPartition.getPreferredLocation())) {

                /* Currently solace can ack messages on consumer flow. So ack previous messages before starting to process new ones.
                 * If Spark starts new input partition it indicates previous batch of data is successful. So we can acknowledge messages here.
                 * Solace connection is always active and acknowledgements should be successful. It might throw exception if connection is lost
                 * */
                log.info("SolaceSparkConnector - Acknowledging any processed messages to Solace as commit is successful");
                long startTime = System.currentTimeMillis();
                SolaceMessageTracker.ackMessages(solaceInputPartition.getId());
                log.trace("SolaceSparkConnector - Total time taken to acknowledge messages {} ms", (System.currentTimeMillis() - startTime));

                log.info("SolaceSparkConnector - Looks like task got reassigned to new executor, closing existing connection to Solace and create new one");
                SolaceConnectionManager.close(solaceInputPartition.getId());
                SolaceBroker solaceBroker = new SolaceBroker(properties, "consumer");
                createNewConnection(solaceBroker, solaceInputPartition.getId(), ackLastProcessedMessages);
                SolaceConnectionManager.addConnection(solaceInputPartition.getId(), solaceBroker);
                SolaceConnectionManager.addPartition(solaceInputPartition.getId(), solaceInputPartition.getPreferredLocation());
                log.info("SolaceSparkConnector - New connection added to Connection Manager for partition {} in executor {}", solaceInputPartition.getId(), solaceInputPartition.getPreferredLocation());
            } else if(SolaceConnectionManager.getConnection(solaceInputPartition.getId()) == null) {
                log.info("SolaceSparkConnector - Creating new connection to Solace for partition {} in executor {}", solaceInputPartition.getId(), solaceInputPartition.getPreferredLocation());
                SolaceBroker solaceBroker = new SolaceBroker(properties, "consumer");
                createNewConnection(solaceBroker, solaceInputPartition.getId(), ackLastProcessedMessages);
                SolaceConnectionManager.addConnection(solaceInputPartition.getId(), solaceBroker);
                SolaceConnectionManager.addPartition(solaceInputPartition.getId(), solaceInputPartition.getPreferredLocation());
            } else {
                log.info("SolaceSparkConnector - Connection already exists for partition {} in executor {}", solaceInputPartition.getId(), solaceInputPartition.getPreferredLocation());
                if(SolaceConnectionManager.getPartition(solaceInputPartition.getId()) == null) {
                    SolaceConnectionManager.addPartition(solaceInputPartition.getId(), solaceInputPartition.getPreferredLocation());
                }
            }
            log.info("SolaceSparkConnector - Creating reader for input partition reader factory with query id {}, batch id {}, task id {} and partition id {}", queryId, batchId, taskCtx.taskAttemptId(), taskCtx.partitionId());

            return new SolaceInputPartitionReader(solaceInputPartition, includeHeaders, properties, taskCtx, checkpoints, checkpointLocation);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createNewConnection(SolaceBroker solaceBroker, String inputPartitionId, boolean ackLastProcessedMessages) {
        log.info("SolaceSparkConnector - Solace Connection Details Host : {}, VPN : {}, Username : {}", properties.get(SolaceSparkStreamingProperties.HOST), properties.get(SolaceSparkStreamingProperties.VPN), properties.get(SolaceSparkStreamingProperties.USERNAME));
        try {
            initProducerAndConsumer(solaceBroker, inputPartitionId, ackLastProcessedMessages);
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception Initializing Solace Broker", solaceBroker.getException() != null ? solaceBroker.getException() : e);
            solaceBroker.close();
            throw new SolaceConsumerException(e);
        }
    }

    private void initProducerAndConsumer(SolaceBroker solaceBroker, String inputPartitionId, boolean ackLastProcessedMessages) {
        solaceBroker.initProducer();
        createReceiver(solaceBroker, inputPartitionId, ackLastProcessedMessages);
    }

    private void createReceiver(SolaceBroker solaceBroker, String inputPartitionId, boolean ackLastProcessedMessages) {
        EventListener eventListener = new EventListener(inputPartitionId);
        if(ackLastProcessedMessages) {
            log.info("SolaceSparkConnector - Ack last processed messages is set to true, connector will match incoming messages with checkpoint and auto acknowledge");
//            List<String> messageIDs = Arrays.stream(this.lastKnownOffset.split(",")).collect(Collectors.toList());
            eventListener = new EventListener(inputPartitionId, this.checkpoints, this.properties.getOrDefault(SolaceSparkStreamingProperties.OFFSET_INDICATOR, SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT));
        }
        // Initialize connection to Solace Broker
        solaceBroker.addReceiver(eventListener);
    }
}
