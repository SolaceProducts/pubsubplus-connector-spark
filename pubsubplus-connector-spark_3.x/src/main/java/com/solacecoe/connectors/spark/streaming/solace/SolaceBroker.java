package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceClassLoader;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceSessionEventListener;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.Queue;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceBroker implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceBroker.class);
    private final String queue;
    private final CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;
    private final JCSMPSession session;
    private XMLMessageProducer producer;

    public SolaceBroker(String host, String vpn, String username, String password, String queue, Map<String, String> properties) {
        eventListeners = new CopyOnWriteArrayList<>();
        flowReceivers = new CopyOnWriteArrayList<>();
        this.queue = queue;

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
            session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties, null, new SolaceSessionEventListener(this));
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

            Context defaultContext = JCSMPFactory.onlyInstance().getDefaultContext();

            Context context = JCSMPFactory.onlyInstance().createContext(null);

            FlowReceiver cons = this.session.createFlow(eventListener,
                    flow_prop, endpoint_props);

            cons.start();
            log.info("SolaceSparkConnector - Consumer flow started to listen for messages on queue " + this.queue);
            flowReceivers.add(cons);
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Consumer received exception. Shutting down consumer ", e);
            close();
            throw new RuntimeException(e);
        }
        // log.info("Listening for messages: "+ this.queueName);
    }

    public void initProducer(JCSMPStreamingPublishCorrelatingEventHandler jcsmpStreamingPublishCorrelatingEventHandler) {
        try {
            this.producer = this.session.getMessageProducer(jcsmpStreamingPublishCorrelatingEventHandler);
        } catch (JCSMPException e) {
            log.error("SolaceSparkConnector - Error creating publisher to Solace", e);
            throw new RuntimeException(e);
        }
    }

    public void publishMessage(String topic, Object msg, UnsafeMapData headersMap) {
        Map<String, Object> headers = new HashMap<>();
        if(headersMap != null && headersMap.numElements() > 0) {
            for (int i = 0; i < headersMap.numElements(); i++) {
                headers.put(headersMap.keyArray().get(i, DataTypes.StringType).toString(),
                        headersMap.valueArray().get(i, DataTypes.BinaryType));
            }
        }
        try {
            XMLMessage xmlMessage = SolaceUtils.map(msg, headers, UUID.randomUUID(), new ArrayList<>(), false);
//            xmlMessage.writeBytes(msg.toString().getBytes(StandardCharsets.UTF_8));
//            xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
            xmlMessage.setCorrelationId(UUID.randomUUID().toString());
            xmlMessage.setCorrelationKey(UUID.randomUUID().toString());
            Destination destination = JCSMPFactory.onlyInstance().createTopic(topic);
            this.producer.send(xmlMessage, destination);
        } catch (SDTException e) {
            throw new RuntimeException(e);
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
    }

    public void close() {
        closeProducer();
        closeReceivers();
        log.info("Closing Solace Session");
        if(session != null && !session.isClosed()) {
            session.closeSession();
            log.info("SolaceSparkConnector - Closed Solace session");
        }
    }

    public ConcurrentLinkedQueue<SolaceMessage> getMessages(int index) {
        log.info("Requesting messages from event listener {}, total messages available :: {}", index, this.eventListeners.get(index).getMessages().size());
        return index < this.eventListeners.size() ? this.eventListeners.get(index).getMessages() : null;
    }

    @Override
    protected void finalize() throws Throwable {
        session.closeSession();
    }
}
