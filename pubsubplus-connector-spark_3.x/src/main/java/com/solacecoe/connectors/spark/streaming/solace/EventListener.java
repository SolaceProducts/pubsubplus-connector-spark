package com.solacecoe.connectors.spark.streaming.solace;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.solacesystems.jcsmp.JCSMPReconnectEventHandler;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventListener implements XMLMessageListener, Serializable {
    private static Logger log = LoggerFactory.getLogger(EventListener.class);
    private final int id;
    private final ConcurrentLinkedQueue<SolaceMessage> messages;
    private SolaceBroker solaceBroker;
    public EventListener(int id) {
        this.id = id;
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public void setBrokerInstance(SolaceBroker solaceBroker) {
        this.solaceBroker = solaceBroker;
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
        if(solaceBroker != null) {
            solaceBroker.handleException("SolaceSparkConnector - Consumer received exception", e);
        } else {
            log.error("SolaceSparkConnector - Consumer received exception: %s%n", e);
            throw new RuntimeException(e);
        }
    }

    public ConcurrentLinkedQueue<SolaceMessage> getMessages() {
        return messages;
    }
}
