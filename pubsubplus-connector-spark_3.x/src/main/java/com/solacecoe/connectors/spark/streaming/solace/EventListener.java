package com.solacecoe.connectors.spark.streaming.solace;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class EventListener implements XMLMessageListener, Serializable {
    private static Logger log = LogManager.getLogger(EventListener.class);
    private final int id;
    private final LinkedBlockingQueue<SolaceMessage> messages;

    public EventListener(int id) {
        this.id = id;
        this.messages = new LinkedBlockingQueue<>();
    }

    @Override
    public void onReceive(BytesXMLMessage msg) {
        try {
            this.messages.add(new SolaceMessage(msg));
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
}
