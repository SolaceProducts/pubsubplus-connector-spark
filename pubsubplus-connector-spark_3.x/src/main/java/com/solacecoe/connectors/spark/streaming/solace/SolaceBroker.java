package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkOffset;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacesystems.jcsmp.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class SolaceBroker implements Serializable {
    private static final Logger log = LogManager.getLogger(SolaceBroker.class);
    private final String queue;
    private final String lvqName;
    private final String lvqTopic;
    private final CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<LVQEventListener> lvqEventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;
    private JCSMPSession session;
    private String uniqueName = "";
    private XMLMessageProducer producer;


    public SolaceBroker(String host, String vpn, String username, String password, String queue, Map<String, String> properties) {
        this.eventListeners = new CopyOnWriteArrayList<>();
        this.lvqEventListeners = new CopyOnWriteArrayList<>();
        this.flowReceivers = new CopyOnWriteArrayList<>();
        this.queue = queue;
        this.lvqName = properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_NAME, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_NAME);
        this.lvqTopic = properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC);
        try {
            JCSMPProperties jcsmpProperties = new JCSMPProperties();
            // get api properties
            Properties props = new Properties();
            for(String key : properties.keySet()) {
                if (key.startsWith("solace.apiProperties.")) {
                    String value = properties.get(key);
                    String solaceKey = key.substring("solace.apiProperties.".length());
                    props.put("jcsmp." + solaceKey, value);
                }
            }
            if(!props.isEmpty()) {
                jcsmpProperties = JCSMPProperties.fromProperties(props);
            }

            jcsmpProperties.setProperty(JCSMPProperties.HOST, host);            // host:port
            jcsmpProperties.setProperty(JCSMPProperties.USERNAME, username); // client-username
            jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, vpn);    // message-vpn
            jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            this.uniqueName = JCSMPFactory.onlyInstance().createUniqueName("solace/spark/connector");
            jcsmpProperties.setProperty(JCSMPProperties.CLIENT_NAME, uniqueName);
            
            // Channel Properties
            JCSMPChannelProperties cp = (JCSMPChannelProperties) jcsmpProperties
                    .getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
            if(properties.containsKey("connectRetries")) {
                cp.setConnectRetries(Integer.parseInt(properties.get("connectRetries")));
            }
            if(properties.containsKey("reconnectRetries")) {
                cp.setReconnectRetries(Integer.parseInt(properties.get("reconnectRetries")));
            }
            if(properties.containsKey("connectRetriesPerHost")) {
                cp.setConnectRetriesPerHost(Integer.parseInt(properties.get("connectRetriesPerHost")));
            }
            if(properties.containsKey("reconnectRetryWaitInMillis")) {
                cp.setReconnectRetryWaitInMillis(Integer.parseInt(properties.get("reconnectRetryWaitInMillis")));
            }
//            jcsmpProperties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, cp);
            session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
            session.connect();
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception connecting to Solace ", e);
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
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
            Queue listenQueue = JCSMPFactory.onlyInstance().createQueue(this.lvqName);
            flow_prop.setEndpoint(listenQueue);
            flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

            EndpointProperties endpoint_props = new EndpointProperties();
            endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
            endpoint_props.setPermission(EndpointProperties.PERMISSION_CONSUME);
            endpoint_props.setQuota(0);
            this.session.provision(listenQueue, endpoint_props, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
            try {
                this.session.addSubscription(listenQueue, JCSMPFactory.onlyInstance().createTopic(this.lvqTopic), JCSMPSession.WAIT_FOR_CONFIRM);
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
                    log.info("SolaceSparkConnector - Message published successfully to Solace");
                }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) {
                    log.error("SolaceSparkConnector - Exception when publishing message to Solace", e);
                    throw new RuntimeException(e);
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
