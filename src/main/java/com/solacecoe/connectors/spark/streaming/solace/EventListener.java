package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceConsumerException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;


public class EventListener implements XMLMessageListener, Serializable {
    private static final Logger log = LoggerFactory.getLogger(EventListener.class);
    private final LinkedBlockingQueue<SolaceMessage> messages;
    private final String id;
    private CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints = new CopyOnWriteArrayList<>();
    private String offsetIndicator = SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT;
    private SolaceBroker solaceBroker;
    public EventListener(String id) {
        this.id = id;
        this.messages = new LinkedBlockingQueue<>();
        log.info("SolaceSparkConnector- Initialized Event listener for Input partition reader with ID {}", id);
    }

    public EventListener(String id, CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints, String offsetIndicator) {
        this.id = id;
        this.messages = new LinkedBlockingQueue<>();
        this.checkpoints = checkpoints;
        this.offsetIndicator = offsetIndicator;
        log.info("SolaceSparkConnector- Initialized Event listener for Input partition reader with ID {}", id);
    }

    public void setBrokerInstance(SolaceBroker solaceBroker) {
        this.solaceBroker = solaceBroker;
    }

    @Override
    public void onReceive(BytesXMLMessage msg) {
        try {
            if(!this.checkpoints.isEmpty()) {
                List<String> lastKnownMessageIDs = new ArrayList<>();
                String messageID = SolaceUtils.getMessageID(msg, this.offsetIndicator);
                boolean hasPartitionKey = msg.getProperties() != null && msg.getProperties().containsKey(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY);
                if(!hasPartitionKey) {
                    for(SolaceSparkPartitionCheckpoint checkpoint : this.checkpoints) {
                        lastKnownMessageIDs.addAll(Arrays.stream(checkpoint.getMessageIDs().split(",")).collect(Collectors.toList()));
                    };

                    if(lastKnownMessageIDs.isEmpty()) {
                        log.warn("SolaceSparkConnector - No message ids available in checkpoint. Message will be reprocessed to ensure reliability—any duplicates must be handled by the downstream system.");
                        this.messages.add(new SolaceMessage(msg));
                    } else {
                        lastKnownMessageIDs.sort((o1, o2) -> {
                            try {
                                return JCSMPFactory.onlyInstance().createReplicationGroupMessageId(o1).compare(JCSMPFactory.onlyInstance().createReplicationGroupMessageId(o2));
                            } catch (JCSMPNotComparableException | InvalidPropertiesException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        compareMessageIds(lastKnownMessageIDs, messageID, msg);
                    }
                } else {
                    String partitionKey = msg.getProperties().getString(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY);
                    if(partitionKey != null && !partitionKey.isEmpty()) {
                        SolaceSparkPartitionCheckpoint solaceSparkPartitionCheckpoint = this.checkpoints.stream().filter(checkpoint -> checkpoint.getPartitionId().equals(partitionKey)).findFirst().orElse(null);
                        if(solaceSparkPartitionCheckpoint != null) {
                            lastKnownMessageIDs = Arrays.stream(solaceSparkPartitionCheckpoint.getMessageIDs().split(",")).collect(Collectors.toList());
                            if(lastKnownMessageIDs.isEmpty()) {
                                log.warn("SolaceSparkConnector - No message ids available in checkpoint. Message will be reprocessed to ensure reliability—any duplicates must be handled by the downstream system.");
                                this.messages.add(new SolaceMessage(msg));
                            } else {
                                compareMessageIds(lastKnownMessageIDs, messageID, msg);
                            }
                        } else {
                            log.warn("SolaceSparkConnector - No checkpoint found for partition key {} and message id {}. Message will be reprocessed to ensure reliability—any duplicates must be handled by the downstream system.", partitionKey, messageID);
                            this.messages.add(new SolaceMessage(msg));
                        }
                    } else {
                        log.warn("SolaceSparkConnector - Incoming message partition key is either null or empty. Message will be reprocessed to ensure reliability—any duplicates must be handled by the downstream system.");
                        this.messages.add(new SolaceMessage(msg));
                    }
                }
            } else {
                this.messages.add(new SolaceMessage(msg));
            }
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception connecting to Solace Queue", e);
            throw new SolaceConsumerException(e);
        }

    }

    private void compareMessageIds(List<String> lastKnownMessageIDs, String messageID, BytesXMLMessage msg) throws JCSMPException {
        for(String msgID : lastKnownMessageIDs) {
            ReplicationGroupMessageId checkpointMsgId = JCSMPFactory.onlyInstance().createReplicationGroupMessageId(msgID);
            ReplicationGroupMessageId currentMessageId = JCSMPFactory.onlyInstance().createReplicationGroupMessageId(messageID);

            if ((currentMessageId.compare(checkpointMsgId) < 0 || currentMessageId.compare(checkpointMsgId) == 0) && lastKnownMessageIDs.size() == 1) {
                msg.ackMessage();
                log.info("SolaceSparkConnector - Acknowledged message with ID {} as user has set ackLastProcessedMessages to true in configuration", messageID);
            } else {
                if (lastKnownMessageIDs.size() > 1) {
                    log.info("SolaceSparkConnector - Checkpoint has more than one message ids {}. This might be due to parallel consumers. The message {} will be acknowledged only if it is older than available message ids else will be sent for reprocessing", lastKnownMessageIDs, currentMessageId);
                    int verificationCount = 0;
                    for(String id: lastKnownMessageIDs) {
                        ReplicationGroupMessageId idToReplicationGroupMessageId = JCSMPFactory.onlyInstance().createReplicationGroupMessageId(id);
                        if ((currentMessageId.compare(idToReplicationGroupMessageId) < 0 || currentMessageId.compare(idToReplicationGroupMessageId) == 0)) {
                            verificationCount++;
                        }
                    }

                    if(verificationCount == lastKnownMessageIDs.size()) {
                        msg.ackMessage();
                        log.info("SolaceSparkConnector - Acknowledged message with ID {} as user has set ackLastProcessedMessages to true in configuration and it is older than checkpoint message ids {}", messageID, lastKnownMessageIDs);
                    } else {
                        this.messages.add(new SolaceMessage(msg));
                        log.info("SolaceSparkConnector - Message Id {} is added for reprocessing as it failed checkpoint validation", currentMessageId);
                    }
                    break;
                } else {
                    this.messages.add(new SolaceMessage(msg));
                    log.info("SolaceSparkConnector - Message Id {} is added for reprocessing as it failed checkpoint validation", currentMessageId);
                }

            }
        }
    }

    @Override
    public void onException(JCSMPException e) {
        if(solaceBroker != null) {
            solaceBroker.handleException("SolaceSparkConnector - Consumer received exception", e);
        } else {
            log.error("SolaceSparkConnector - Consumer received exception: %s%n", e);
            throw new SolaceConsumerException(e);
        }
    }

    public LinkedBlockingQueue<SolaceMessage> getMessages() {
        return messages;
    }

    public String getId() {
        return id;
    }
}
