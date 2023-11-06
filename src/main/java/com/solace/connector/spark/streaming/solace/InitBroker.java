package com.solace.connector.spark.streaming.solace;

import com.solacesystems.jcsmp.*;

import java.io.Serializable;

public class InitBroker implements Serializable {

    private JCSMPSession session;
    private String host;
    private String vpn;
    private String username;
    private String password;
    private String queue;

    private FlowReceiver cons;

    public InitBroker(String host, String vpn, String username, String password, String queue) {
        this.host = host;
        this.vpn = vpn;
        this.username = username;
        this.password = password;
        this.queue = queue;

        try {
            final JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, this.host);            // host:port
            properties.setProperty(JCSMPProperties.USERNAME, this.username); // client-username
            properties.setProperty(JCSMPProperties.VPN_NAME, this.vpn);    // message-vpn
            properties.setProperty(JCSMPProperties.PASSWORD, this.password); // client-password
            session = JCSMPFactory.onlyInstance().createSession(properties);
            session.connect();
        } catch (Exception e) {

        }
    }

    public void setReceiver(EventListener eventListener) {

        try {
            ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
            Queue listenQueue = JCSMPFactory.onlyInstance().createQueue(this.queue);

            flow_prop.setEndpoint(listenQueue);
            flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

            EndpointProperties endpoint_props = new EndpointProperties();
            endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

            Context defaultContext = JCSMPFactory.onlyInstance().getDefaultContext();

            Context context = JCSMPFactory.onlyInstance().createContext(null);

            cons = this.session.createFlow(eventListener,
                    flow_prop, endpoint_props);

            cons.start();
        } catch (Exception e) {

        }
        // log.info("Listening for messages: "+ this.queueName);
    }

    public void close() {
        if(cons != null && !cons.isClosed()) {
            cons.close();
        }

        if(session != null && !session.isClosed()) {
            session.closeSession();
        }
    }


    @Override
    protected void finalize() throws Throwable {
        session.closeSession();
    }
}
