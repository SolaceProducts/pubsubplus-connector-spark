package com.solacecoe.connectors.spark.streaming.solace;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventListener implements XMLMessageListener, Serializable {
    private static Logger log = LoggerFactory.getLogger(EventListener.class);
    private final int id;
    private final String lastKnownMessageId;
    private final int lastKnownMessageIdThreshold;
    private final String offsetIndicator;
    private final ConcurrentLinkedQueue<SolaceMessage> messages;
    private final ConcurrentLinkedQueue<SolaceMessage> tempMessages;
    private int thresholdCounter = 0;
    public EventListener(int id, String lastKnownMessageId, int lastKnownMessageIdThreshold, String offsetIndicator) {
        this.id = id;
        this.lastKnownMessageId = lastKnownMessageId;
        this.lastKnownMessageIdThreshold = lastKnownMessageIdThreshold;
        this.offsetIndicator = offsetIndicator;
        this.messages = new ConcurrentLinkedQueue<>();
        this.tempMessages = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void onReceive(BytesXMLMessage msg) {
        try {
            if(lastKnownMessageId != null) {
                String messageId = SolaceUtils.getMessageID(msg, this.offsetIndicator);
                thresholdCounter++;
                this.tempMessages.add(new SolaceMessage(msg));
                if(lastKnownMessageId.equals(messageId)) {
                    log.info("SolaceSparkConnector - Last Known Message ID: {} is found within threshold limit. Acknowledging {} messages", lastKnownMessageId, this.tempMessages.size());
                    this.tempMessages.forEach(tempMessages -> {
                        tempMessages.bytesXMLMessage.ackMessage();
                    });
                    this.tempMessages.clear();
                    thresholdCounter = lastKnownMessageIdThreshold;
                } else if(thresholdCounter >= lastKnownMessageIdThreshold) {
                    if(!this.tempMessages.isEmpty() && this.messages.isEmpty()) {
                        this.messages.addAll(this.tempMessages);
                        this.tempMessages.clear();
                    } else {
                        this.messages.add(new SolaceMessage(msg));
                    }
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

    public ConcurrentLinkedQueue<SolaceMessage> getMessages() {
        return messages;
    }
}
