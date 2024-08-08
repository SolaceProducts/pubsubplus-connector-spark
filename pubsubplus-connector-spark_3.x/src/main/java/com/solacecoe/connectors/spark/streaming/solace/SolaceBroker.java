package com.solacecoe.connectors.spark.streaming.solace;

import com.solacesystems.jcsmp.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class SolaceBroker implements Serializable {
    private static final Logger log = LogManager.getLogger(SolaceBroker.class);
    private JCSMPSession session;
    private final String queue;
    private CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;

    public SolaceBroker(String host, String vpn, String username, String password, String queue) {
        eventListeners = new CopyOnWriteArrayList<>();
        flowReceivers = new CopyOnWriteArrayList<>();
        this.queue = queue;

        try {
            final JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, host);            // host:port
            properties.setProperty(JCSMPProperties.USERNAME, username); // client-username
            properties.setProperty(JCSMPProperties.VPN_NAME, vpn);    // message-vpn
            properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            String uniqueName = JCSMPFactory.onlyInstance().createUniqueName("solace/spark/connector");
            properties.setProperty(JCSMPProperties.CLIENT_NAME, uniqueName);
            session = JCSMPFactory.onlyInstance().createSession(properties);
            session.connect();
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception connecting to Solace ", e);
        }
    }

    public void addReceiver(EventListener eventListener) {
        eventListeners.add(eventListener);
        setReceiver(eventListener);
    }

    private void setReceiver(EventListener eventListener) {
        try {
            ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
            Queue listenQueue = JCSMPFactory.onlyInstance().createQueue(this.queue);
            flow_prop.setEndpoint(listenQueue);
            flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

            EndpointProperties endpoint_props = new EndpointProperties();
            endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

            Context defaultContext = JCSMPFactory.onlyInstance().getDefaultContext();

            Context context = JCSMPFactory.onlyInstance().createContext(null);

            FlowReceiver cons = this.session.createFlow(eventListener,
                    flow_prop, endpoint_props);

            cons.start();
            flowReceivers.add(cons);
            log.info("SolaceSparkConnector - Consumer flow started to listen for messages on queue {}", this.queue);
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Consumer received exception. Shutting down consumer ", e);
            close();
        }
        // log.info("Listening for messages: "+ this.queueName);
    }

    public void closeReceivers() {
        log.info("Closing {} flow receivers", flowReceivers.size());
        flowReceivers.forEach(flowReceiver -> {
            if(flowReceiver != null && !flowReceiver.isClosed()) {
                String endpoint = flowReceiver.getEndpoint().getName();
                flowReceiver.close();
                log.info("SolaceSparkConnector - Closed flow receiver to endpoint {}", endpoint);
            }
        });
        eventListeners = new CopyOnWriteArrayList<>();
    }

    public void close() {
//        closeReceivers();
        log.info("Closing session");
        if(session != null && !session.isClosed()) {
            session.closeSession();
            log.info("SolaceSparkConnector - Closed Solace session");
        }
    }

    public LinkedBlockingQueue<SolaceMessage> getMessages(int index) {
        if(index < this.eventListeners.size()) {
            log.info("Requesting messages from event listener " + index + ", total messages available :: " + this.eventListeners.get(index).getMessages().size());
            return this.eventListeners.get(index).getMessages();
        }

        return null;
    }

    @Override
    protected void finalize() throws Throwable {
        session.closeSession();
    }
}
