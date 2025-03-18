package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceConsumerException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;


public class EventListener implements XMLMessageListener, Serializable {
    private static Logger log = LoggerFactory.getLogger(EventListener.class);
    private final LinkedBlockingQueue<SolaceMessage> messages;
    private final String id;
    private List<String> lastKnownMessageIDs = new ArrayList<>();
    private String offsetIndicator = SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT;
    private SolaceBroker solaceBroker;
    public EventListener(String id) {
        this.id = id;
        this.messages = new LinkedBlockingQueue<>();
        log.info("SolaceSparkConnector- Initialized Event listener for Input partition reader with ID {}", id);
    }

    public EventListener(String id, List<String> messageIDs, String offsetIndicator) {
        this.id = id;
        this.messages = new LinkedBlockingQueue<>();
        this.lastKnownMessageIDs = messageIDs;
        this.offsetIndicator = offsetIndicator;
        log.info("SolaceSparkConnector- Initialized Event listener for Input partition reader with ID {}", id);
    }

    public void setBrokerInstance(SolaceBroker solaceBroker) {
        this.solaceBroker = solaceBroker;
    }

    @Override
    public void onReceive(BytesXMLMessage msg) {
        try {
            if(!lastKnownMessageIDs.isEmpty()) {
                String messageID = SolaceUtils.getMessageID(msg, this.offsetIndicator);
                for(String msgID : lastKnownMessageIDs) {
                    if(msgID != null && !msgID.isEmpty()) {
                        ReplicationGroupMessageId checkpointMsgId = JCSMPFactory.onlyInstance().createReplicationGroupMessageId(msgID);
                        ReplicationGroupMessageId currentMessageId = JCSMPFactory.onlyInstance().createReplicationGroupMessageId(messageID);

                        if (currentMessageId.compare(checkpointMsgId) < 0 || currentMessageId.compare(checkpointMsgId) == 0) {
                            msg.ackMessage();
                            log.info("SolaceSparkConnector- Acknowledged message with ID {} present in last known offset as user has set ackLastProcessedMessages to true in configuration", messageID);
                        }
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
