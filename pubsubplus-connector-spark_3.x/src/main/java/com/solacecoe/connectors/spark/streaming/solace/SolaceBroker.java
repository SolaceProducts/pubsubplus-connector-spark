package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacesystems.jcsmp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class SolaceBroker implements Serializable, JCSMPReconnectEventHandler {
    private static final Logger log = LoggerFactory.getLogger(SolaceBroker.class);
    private final JCSMPSession session;
    private final String queue;
    private OAuthClient oAuthClient;
    private final CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;
    private ScheduledExecutorService scheduledExecutorService;
    public SolaceBroker(String host, String vpn, String username, String password, String queue, Map<String, String> properties) {
        eventListeners = new CopyOnWriteArrayList<>();
        flowReceivers = new CopyOnWriteArrayList<>();
        this.queue = queue;

        try {
            JCSMPProperties jcsmpProperties = new JCSMPProperties();
            // get api properties
            Properties props = new Properties();
            for(String key : properties.keySet()) {
                if (key.startsWith(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX)) {
                    String value = properties.get(key);
                    String solaceKey = key.substring(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX.length());
                    props.put("jcsmp." + solaceKey, value);
                }
            }
            if(!props.isEmpty()) {
                jcsmpProperties = JCSMPProperties.fromProperties(props);
            }

            jcsmpProperties.setProperty(JCSMPProperties.HOST, host);            // host:port
            jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, vpn);    // message-vpn
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+JCSMPProperties.AUTHENTICATION_SCHEME) && properties.get(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+JCSMPProperties.AUTHENTICATION_SCHEME).equals(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)) {
                int fetchTimeout = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT, SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT_DEFAULT));
                boolean validateSSLCertificate = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, "false"));
                String trustStoreFilePath = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_FILE, null);
                String trustStoreFilePassword = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, null);
                String tlsVersion = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TLS_VERSION, SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TLS_VERSION_DEFAULT);

                oAuthClient = new OAuthClient(properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL), properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID),
                        properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET));
                oAuthClient.buildRequest(fetchTimeout, properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, null), trustStoreFilePath, trustStoreFilePassword, tlsVersion, validateSSLCertificate);

                jcsmpProperties.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oAuthClient.getAccessToken().getValue());
                scheduleOAuthRefresh(Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL_DEFAULT)));
            } else {
                jcsmpProperties.setProperty(JCSMPProperties.USERNAME, username); // client-username
                jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            }

            // Channel Properties
            JCSMPChannelProperties cp = (JCSMPChannelProperties) jcsmpProperties
                    .getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES)) {
                cp.setConnectRetries(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES)));
            }
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES)) {
                cp.setReconnectRetries(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES)));
            }
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST)) {
                cp.setConnectRetriesPerHost(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST)));
            }
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME)) {
                cp.setReconnectRetryWaitInMillis(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME)));
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

            Context defaultContext = JCSMPFactory.onlyInstance().getDefaultContext();

            Context context = JCSMPFactory.onlyInstance().createContext(null);

            FlowReceiver cons = this.session.createFlow(eventListener,
                    flow_prop, endpoint_props);

            cons.start();
            log.info("SolaceSparkConnector - Consumer flow started to listen for messages on queue " + this.queue);
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Consumer received exception. Shutting down consumer ", e);
            close();
            throw new RuntimeException(e);
        }
        // log.info("Listening for messages: "+ this.queueName);
    }

    public void close() {
        flowReceivers.forEach(flowReceiver -> {
            if(flowReceiver != null && !flowReceiver.isClosed()) {
                String endpoint = flowReceiver.getEndpoint().getName();
                flowReceiver.close();
                log.info("SolaceSparkConnector - Closed flow receiver to endpoint " + endpoint);
            }
        });


        if(session != null && !session.isClosed()) {
            session.closeSession();
            log.info("SolaceSparkConnector - Closed Solace session");
        }

        if(scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            log.info("SolaceSparkConnector - OAuth token refresh thread is now shutdown");
        }
    }

    public ConcurrentLinkedQueue<SolaceMessage> getMessages(int index) {
        log.info("Requesting messages from event listener " + index + ", total messages available :: " + this.eventListeners.get(index).getMessages().size());
        return index < this.eventListeners.size() ? this.eventListeners.get(index).getMessages() : null;
    }

    @Override
    protected void finalize() throws Throwable {
        session.closeSession();
    }

    @Override
    public boolean preReconnect() throws JCSMPException {
        if(session != null && oAuthClient != null) {
            session.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oAuthClient.getAccessToken().getValue());
        }

        return true;
    }

    @Override
    public void postReconnect() throws JCSMPException {

    }

    private void scheduleOAuthRefresh(int refreshInterval) {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if(session != null && oAuthClient != null) {
                try {
                    session.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oAuthClient.getAccessToken().getValue());
                } catch (JCSMPException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, refreshInterval, TimeUnit.SECONDS);
    }
}
