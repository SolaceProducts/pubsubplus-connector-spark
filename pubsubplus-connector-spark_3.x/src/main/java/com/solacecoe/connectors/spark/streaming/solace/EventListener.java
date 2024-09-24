package com.solacecoe.connectors.spark.streaming.solace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class EventListener implements XMLMessageListener, Serializable {
    private static final Logger log = LogManager.getLogger(EventListener.class);
    private final LinkedBlockingQueue<SolaceMessage> messages;
    private final String id;
    private List<String> lastKnownMessageIDs = new ArrayList<>();
    private String offsetIndicator = SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT;
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

    @Override
    public void onReceive(BytesXMLMessage msg) {
        try {
            if(!lastKnownMessageIDs.isEmpty()) {
                String messageID = SolaceUtils.getMessageID(msg, this.offsetIndicator);
                if(lastKnownMessageIDs.contains(messageID)) {
                    log.info("SolaceSparkConnector- Acknowledging message with ID {} as it is present in last known offset and user has set ackLastProcessedMessages to true in configuration", messageID);
                    msg.ackMessage();
                    log.info("SolaceSparkConnector- Acknowledged message with ID {} present in last known offset", messageID);
                }
            } else {
                this.messages.add(new SolaceMessage(msg));
            }
//            log.info("Current messages in consumer "+this.id+" is :: " + this.messages.size());
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception connecting to Solace Queue", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void onException(JCSMPException e) {
        log.error("SolaceSparkConnector - Consumer received exception: %s%n", e);
        throw new RuntimeException(e);
    }

    public LinkedBlockingQueue<SolaceMessage> getMessages() {
        return messages;
    }

    public String getId() {
        return id;
    }
}
