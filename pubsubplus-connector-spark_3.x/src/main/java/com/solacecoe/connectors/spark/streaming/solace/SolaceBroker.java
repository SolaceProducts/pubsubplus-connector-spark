package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkOffset;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacesystems.jcsmp.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class SolaceBroker implements Serializable {
    private static final Logger log = LogManager.getLogger(SolaceBroker.class);
    private JCSMPSession session;
    private final String queue;
    private String uniqueName = "";
    private final CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<LVQEventListener> lvqEventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;
    private XMLMessageProducer producer;


    public SolaceBroker(String host, String vpn, String username, String password, String queue) {
        eventListeners = new CopyOnWriteArrayList<>();
        lvqEventListeners = new CopyOnWriteArrayList<>();
        flowReceivers = new CopyOnWriteArrayList<>();
        this.queue = queue;

        try {
            final JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, host);            // host:port
            properties.setProperty(JCSMPProperties.USERNAME, username); // client-username
            properties.setProperty(JCSMPProperties.VPN_NAME, vpn);    // message-vpn
            properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            this.uniqueName = JCSMPFactory.onlyInstance().createUniqueName("solace/spark/connector");
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

    public void addLVQReceiver(LVQEventListener lvqEventListener) {
        lvqEventListeners.add(lvqEventListener);
        setLVQReceiver(lvqEventListener);
    }

    private void setLVQReceiver(LVQEventListener eventListener) {
        try {
            ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
            Queue listenQueue = JCSMPFactory.onlyInstance().createQueue(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_NAME);
            flow_prop.setEndpoint(listenQueue);
            flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

            EndpointProperties endpoint_props = new EndpointProperties();
            endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
            endpoint_props.setPermission(EndpointProperties.PERMISSION_CONSUME);
            endpoint_props.setQuota(0);
            this.session.provision(listenQueue, endpoint_props, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
            try {
                this.session.addSubscription(listenQueue, JCSMPFactory.onlyInstance().createTopic(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC), JCSMPSession.WAIT_FOR_CONFIRM);
            } catch (JCSMPException e) {
                log.warn("SolaceSparkConnector - Subscription already exists on LVQ. Ignoring error");
            }
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

    public void initProducer() {
        try {
            this.producer = this.session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object o) {

                }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) {

                }
            });
        } catch (JCSMPException e) {
            log.error("SolaceSparkConnector - Error creating publisher to Solace", e);
            throw new RuntimeException(e);
        }
    }

    public void publishMessage(String topic, Object msg) {
        BytesXMLMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
        xmlMessage.writeBytes(msg.toString().getBytes(StandardCharsets.UTF_8));
        xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
        Destination destination = JCSMPFactory.onlyInstance().createTopic(topic);
        try {
            this.producer.send(xmlMessage, destination);
        } catch (JCSMPException e) {
            log.error("SolaceSparkConnector - Error publishing connector state to Solace", e);
            throw new RuntimeException(e);
        }
    }

    public void closeProducer() {
        if(this.producer != null && !this.producer.isClosed()) {
            this.producer.close();
            log.info("SolaceSparkConnector - Solace Producer closed");
        }
    }

    public void closeReceivers() {
        log.info("SolaceSparkConnector - Closing {} flow receivers", flowReceivers.size());
        flowReceivers.forEach(flowReceiver -> {
            if(flowReceiver != null && !flowReceiver.isClosed()) {
                String endpoint = flowReceiver.getEndpoint().getName();
                flowReceiver.close();
                log.info("SolaceSparkConnector - Closed flow receiver to endpoint {}", endpoint);
            }
        });
        flowReceivers.clear();
        eventListeners.clear();
        lvqEventListeners.clear();
    }

    public void close() {
        closeReceivers();
        log.info("Closing Solace Session");
        if(session != null && !session.isClosed()) {
            session.closeSession();
            log.info("SolaceSparkConnector - Closed Solace session");
        }
    }

    public LinkedBlockingQueue<SolaceMessage> getMessages(int index) {
        if(index < this.eventListeners.size()) {
            log.info("SolaceSparkConnector - Requesting messages from event listener {} with session {}, total messages available :: {}", index, this.uniqueName, this.eventListeners.get(index).getMessages().size());
            return this.eventListeners.get(index).getMessages();
        }

        return null;
    }

    public SolaceSparkOffset getLVQMessage() {
        if(!lvqEventListeners.isEmpty() && this.lvqEventListeners.get(0).getSolaceSparkOffsets().length > 0) {
            log.info("Requesting messages from lvq listener, total messages available :: {}", this.lvqEventListeners.get(0).getSolaceSparkOffsets().length);
            return this.lvqEventListeners.get(0).getSolaceSparkOffsets()[0];
        }

        return null;

    }

    @Override
    protected void finalize() {
        close();
    }

    public String getUniqueName() {
        return uniqueName;
    }
}
