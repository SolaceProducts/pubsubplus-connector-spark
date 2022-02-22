package com.solace.connector.spark;

import com.google.gson.Gson;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.*;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SolaceSparkMicroBatch implements MicroBatchStream, SupportsAdmissionControl, Serializable {

    private static final org.apache.log4j.Logger log = Logger.getLogger(SolaceSparkMicroBatch.class);
    private final Map<String, com.solace.connector.spark.Message> messages = new ConcurrentHashMap<>();
    private final Map<Integer, List<com.solace.connector.spark.Message>> batches = new ConcurrentHashMap<>();
    private final ArrayList<SolaceInputPartition> solaceInputPartitions = new ArrayList<>();
    private Map<Integer, Integer> mapBatchToPartitions = new ConcurrentHashMap<>();
    private List<com.solace.connector.spark.Message> rowChunk = new ArrayList<>();
    private String currentOffset = "0";
    private String lastOffsetCommitted = "";
    private int size = 0;
    private int batchCount = 0;

    private boolean loadInitialOffset = true;

    private final ReentrantLock reentrantLock = new ReentrantLock();
    private boolean isException = false;

    private final SolaceReader solaceReader;

    public SolaceSparkMicroBatch(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        log.info("SolaceSparkConnector - Initializing");
        size = Integer.parseInt(properties.get("batchSize"));
        solaceReader = new SolaceReader(properties.get("host"), properties.get("vpn"), properties.get("username"), properties.get("password"), properties.get("queue"));
        Thread thread = new Thread(solaceReader);
        thread.setName(properties.get("queue") + " thread");
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((t, e) -> {
            isException = true;
            log.error("SolaceSparkConnector - Exception in thread " + t.getName() + " Failed with exception " + e.getMessage());
        });
        thread.start();
    }

    @Override
    public Offset latestOffset() {
        throw new RuntimeException("");
    }

    @Override
    public Offset latestOffset(Offset start, ReadLimit readLimit) {
        try {
            ArrayList<com.solace.connector.spark.Message> temp = new ArrayList<>();
            reentrantLock.lock();
            Iterator<Map.Entry<String, com.solace.connector.spark.Message>> iterator = messages.entrySet().iterator();
            Map.Entry<String, com.solace.connector.spark.Message> key;
            int k = 0;
            while (iterator.hasNext()) {
                key = iterator.next();
                com.solace.connector.spark.Message value = key.getValue();
                try {
                    if (lastOffsetCommitted.length() > 0 && lastOffsetCommitted.contains(",") && Arrays.asList(lastOffsetCommitted.split(",")).contains(value.message.getMessageId())) {
                        log.info("SolaceSparkConnector - Acknowledging previously processed message " + value.message.getMessageId());
                        value.message.ackMessage();
                        iterator.remove();
                    } else {
                        k = k + 1;
                        temp.add(value);
                        if (k == size || !iterator.hasNext()) {
                            currentOffset = "";
                            List<String> msgIDs = temp.stream().map(value1 -> value1.message.getMessageId()).collect(Collectors.toList());
                            currentOffset = String.join(",", msgIDs);
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("SolaceSparkConnector - Error converting message to Solace Text Record " + e.getMessage());
                    throw new RuntimeException(e.getMessage());
                }
            }
            temp = new ArrayList<>();
        } finally {
            reentrantLock.unlock();
        }

        return new SolaceOffset(currentOffset);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        //InputPartition[] partition = new InputPartition[0];
        if (!isException) {
            log.info("SolaceSparkConnector - Creating Input Partitions");
            rowChunk = new ArrayList<>();
            String endOffset = ((SolaceOffset) end).getOffset();
            try {
                int k = 0;
                reentrantLock.lock();
                Iterator<Map.Entry<String, com.solace.connector.spark.Message>> iterator = messages.entrySet().iterator();
                Map.Entry<String, com.solace.connector.spark.Message> key;
                while (iterator.hasNext()) {
                    key = iterator.next();
                    com.solace.connector.spark.Message value = key.getValue();
                    try {
//                        String messageIDs = "";
//                        if(lastOffsetCommitted != null && lastOffsetCommitted.contains(":") && lastOffsetCommitted.split(":").length > 1) {
//                            messageIDs = lastOffsetCommitted.split(":")[1];
//                        }

                        if (lastOffsetCommitted.length() > 0 && lastOffsetCommitted.contains(",") && Arrays.asList(lastOffsetCommitted.split(",")).contains(value.message.getMessageId())) {
                            log.info("SolaceSparkConnector - Acknowledging previously processed message " + value.message.getMessageId());
                            value.message.ackMessage();
                            iterator.remove();
                        } else if (endOffset.contains(",") && Arrays.asList(endOffset.split(",")).contains(value.message.getMessageId())) {
                            rowChunk.add(value);
                            k = k + 1;
                            iterator.remove();
                            if (k == size || !iterator.hasNext()) {
                                batches.put(batchCount, rowChunk);
//                                currentOffset = "";
//                                List<String> msgIDs = rowChunk.stream().map(value1 -> value1.message.getApplicationMessageId()).collect(Collectors.toList());
//                                currentOffset = String.join(",", msgIDs);
                                ArrayList<SolaceRecord> partitionData = new ArrayList(rowChunk.stream().map(message -> {
                                    try {
                                        return SolaceRecord.getMapper().map(message.message);
                                    } catch (Exception exception) {
                                        log.error("Error mapping to solace records. " + exception.getMessage());
                                        throw new RuntimeException(exception.getMessage());
                                    }
                                }).collect(Collectors.toList()));
                                solaceInputPartitions.add(new SolaceInputPartition(partitionData, Integer.toString(batchCount)));
                                mapBatchToPartitions.put(batchCount, solaceInputPartitions.size() - 1);
                                batchCount++;
                                break;
                            }
                        }
                    } catch (Exception e) {
                        log.error("SolaceSparkConnector - Error converting message to Solace Text Record " + e.getMessage());
                        throw new RuntimeException(e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.error("SolaceSparkConnector - Error while creating partitions " + e.getMessage());
                System.exit(0);
            } finally {
                reentrantLock.unlock();
            }
            log.info("SolaceSparkConnector - Created Input Partitions of size " + solaceInputPartitions.size());
            return solaceInputPartitions.toArray(new SolaceInputPartition[]{});
        } else {
            log.info("SolaceSparkConnector - Exception encountered, skipping input partitions");
            throw new RuntimeException();
        }
    }


    @Override
    public PartitionReaderFactory createReaderFactory() {
        if (!isException) {
            log.info("SolaceSparkConnector - Creating Reader factory");
            return new SolacePartitionReaderFactory();
        } else {
            log.info("SolaceSparkConnector - Exception encountered, skipping create reader factory");
            throw new RuntimeException();
        }
    }

    @Override
    public Offset initialOffset() {
        log.info("SolaceSparkConnector - Initial Offset: 0");
        return new SolaceOffset("0");
    }

    @Override
    public Offset deserializeOffset(String json) {
        if (!isException) {
            Gson gson = new Gson();
            SolaceOffset offset = gson.fromJson(json, SolaceOffset.class);
            lastOffsetCommitted = offset.getOffset();
            currentOffset = offset.getOffset();
            return offset;
        }

        log.info("SolaceSparkConnector - Exception encountered, skipping input deserialize offset");
        throw new RuntimeException();
    }

    @Override
    public void commit(Offset end) {
        try {
            if (!isException) {
                log.info("SolaceSparkConnector - Commit triggered from Spark. Verify and acknowledge message to Solace");

                SolaceOffset committedOffset = (SolaceOffset) end;
                if (!loadInitialOffset) {
                    committedOffset = (SolaceOffset) end;
                }

                log.info("SolaceSparkConnector - Last Committed Offset " + committedOffset);
                boolean removeBatch = false;
                Iterator<Map.Entry<Integer, List<com.solace.connector.spark.Message>>> iterator = batches.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Integer, List<com.solace.connector.spark.Message>> message = iterator.next();
                    String committedOffsetStr = committedOffset.getOffset();
                    ListIterator<com.solace.connector.spark.Message> messageIterator = message.getValue().listIterator();
                    while (messageIterator.hasNext()) {
                        com.solace.connector.spark.Message msg = messageIterator.next();
                        if ((committedOffsetStr.length() > 0 && committedOffsetStr.contains(",") && Arrays.asList(committedOffsetStr.split(",")).contains(msg.message.getMessageId()))) {
                            msg.message.ackMessage();
                            log.info("SolaceSparkConnector - Acknowledged Solace Message with ID " + msg.message.getMessageId());
                            removeBatch = true;
                        } else {
                            removeBatch = false;
                        }
                    }
                    if (removeBatch) {
                        log.info("SolaceSparkConnector - Acknowledging Solace messages completed");
                        lastOffsetCommitted = committedOffset.getOffset();
                        //endOffset = lastOffsetCommitted;
                        log.info("SolaceSparkConnector - Clearing input partition");
                        int index = mapBatchToPartitions.get(message.getKey());
                        if (solaceInputPartitions.size() > index && solaceInputPartitions.get(index) != null) {
                            solaceInputPartitions.remove(index);
                        }
                        iterator.remove();
                    }
                }
                loadInitialOffset = false;
            }
//        return new InputPartition[0];
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Error while acknowledging committed offsets " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void stop() {
        log.info("SolaceSparkConnector - Closing Solace Session");
        solaceReader.close();
        log.info("SolaceSparkConnector - Successfully Closed Solace Session");
    }

    //--------- Solace Reader ----------

    class SolaceReader implements Runnable, Serializable {

        private final String hostName;
        private final String vpnName;
        private final String username;
        private final String password;
        private final String queue;
        private boolean isRunning;
        private JCSMPSession session;
        private FlowReceiver flowReceiver;
        private ConsumerFlowProperties flow_prop;

        public SolaceReader(String host, String vpn, String username, String password, String queue) {
            this.hostName = host;
            this.vpnName = vpn;
            this.username = username;
            this.password = password;
            this.queue = queue;
            this.isRunning = true;
        }

        private void initialize() throws Exception {
            try {
                log.info("SolaceSparkConnector - Initializing connection to Solace and establishing connection to " + this.queue + " Queue");
                JCSMPProperties jcsmpProperties = new JCSMPProperties();
                jcsmpProperties.setProperty(JCSMPProperties.HOST, this.hostName);
                jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, this.vpnName);
                jcsmpProperties.setProperty(JCSMPProperties.USERNAME, this.username);
                jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, this.password);

                session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
                session.connect();
                log.info("SolaceSparkConnector - Connection Established to Solace");
                setUpQueue();
            } catch (InvalidPropertiesException e) {
                log.error("SolaceSparkConnector - Invalid Solace Properties " + e.getMessage());
                close();
                throw new RuntimeException(e.getMessage());
            } catch (JCSMPException e) {
                log.error("SolaceSparkConnector - Error connecting to Solace " + e.getMessage());
                close();
                throw new RuntimeException(e.getMessage());
            }
        }

        private void setUpQueue() throws Exception {
            try {
                Queue queue = JCSMPFactory.onlyInstance().createQueue(this.queue);
                flow_prop = new ConsumerFlowProperties();
                // Create a Flow be able to bind to and consume messages from the Queue.
                flow_prop.setEndpoint(queue);
                // will ack the messages in checkpoint
                flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
                // bind to the queue, passing null as message listener for no async callback
                flowReceiver = session.createFlow(null, flow_prop, new EndpointProperties());
                flowReceiver.start();
                log.info("SolaceSparkConnector - Connected to " + this.queue + " Queue");
                consume();
            } catch (JCSMPException e) {
                log.error("SolaceSparkConnector - Error connecting to Queue " + e.getMessage());
                close();
                throw new Exception(e.getMessage());
            }
        }

        @Override
        public void run() {
            if (isRunning) {
                try {
                    initialize();
                } catch (Exception e) {
                    close();
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        private void consume() {
            while (isRunning) {
                try {
                    if(flowReceiver != null && !flowReceiver.isClosed()) {
                        BytesXMLMessage msg = flowReceiver.receive();
                        if (msg != null) {
                            reentrantLock.lock();
                            synchronized (messages) {
                                log.info("SolaceSparkConnector - Received MessageID String while reading - " + msg.getMessageId());
                                log.info("SolaceSparkConnector - Received MessageID Long while reading - " + msg.getMessageIdLong());
                                messages.put(msg.getMessageId(), new Message(msg, Instant.now()));
                            }
                        }
                    }
                } catch (Exception w) {
                    if(w instanceof NullPointerException) {
                        log.error("SolaceSparkConnector - Exception while reading messages from Solace. Check if message id is null " + w);
                        throw new RuntimeException(w.getMessage());
                    } else {
                        log.error("SolaceSparkConnector - Exception connecting to Solace " + w.toString());
                        try {
                            log.info("SolaceSparkConnector - Retrying connection");
                            flowReceiver.close();
                            flowReceiver = session.createFlow(null, flow_prop, new EndpointProperties());
                            flowReceiver.start();
                            log.info("SolaceSparkConnector - Reconnected successfully");
                        } catch (Exception restartEx) {
                            log.error("SolaceSparkConnector - Error while reconnecting to Solace " + restartEx.getMessage());
                            close();
                            throw new RuntimeException(restartEx.getMessage());
                        }
                    }
                } finally {
                    if (reentrantLock.isLocked()) {
                        reentrantLock.unlock();
                    }
                }
            }
        }

        private void close() {
            try {
                this.isRunning = false;
                if (this.flowReceiver != null) {
                    log.info("SolaceSparkConnector - Closing flow receiver for Queue " + this.queue);
                    this.flowReceiver.close();
                }

                if (this.session != null && !this.session.isClosed()) {
                    log.info("SolaceSparkConnector - Closing Solace session");
                    this.session.closeSession();
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }


}

