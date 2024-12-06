package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceSessionEventListener;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidAccessTokenException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceSessionException;
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.Queue;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class SolaceBroker implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceBroker.class);
    private final JCSMPSession session;
    private final String queue;
    private OAuthClient oAuthClient;
    private final CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;
    private XMLMessageProducer producer;

    private ScheduledExecutorService scheduledExecutorService;
    private boolean isException;
    private Exception exception;
    private long accessTokenSourceLastModifiedTime = 0L;
    private boolean isAccessTokenSourceModified = true;
    private boolean isOAuth = false;
    public SolaceBroker(String host, String vpn, String username, String password, String queue, Map<String, String> properties) {
        eventListeners = new CopyOnWriteArrayList<>();
        flowReceivers = new CopyOnWriteArrayList<>();
        this.queue = queue;

        try {
            JCSMPProperties jcsmpProperties = new JCSMPProperties();
            // get api properties
            Properties props = new Properties();
            for(Map.Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey().startsWith(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX)) {
                    String value = entry.getValue();
                    String solaceKey = entry.getKey().substring(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX.length());
                    props.put("jcsmp." + solaceKey, value);
                }
            }
            if(!props.isEmpty()) {
                jcsmpProperties = JCSMPProperties.fromProperties(props);
            }

            jcsmpProperties.setProperty(JCSMPProperties.HOST, host);            // host:port
            jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, vpn);    // message-vpn
            jcsmpProperties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 50); // default window size for publishing
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+JCSMPProperties.AUTHENTICATION_SCHEME) && properties.get(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX+JCSMPProperties.AUTHENTICATION_SCHEME).equals(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)) {
                isOAuth = true;
                int interval = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL, SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_REFRESH_INTERVAL_DEFAULT));
                // if access token is configured, read it directly from source
                if(properties.containsKey(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN)) {
                    String accessTokenSourceType = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN_SOURCE, SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN_SOURCE_DEFAULT);
                    // default file source is currently supported
                    if(accessTokenSourceType.equals(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN_SOURCE_DEFAULT)) {
                        String accessTokenSource = properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_ACCESSTOKEN);
                        String accessToken = readAccessTokenFromFile(accessTokenSource);
                        jcsmpProperties.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, accessToken);
                        scheduleOAuthRefresh(accessTokenSource, interval);
                    }
                } else {
                    int fetchTimeout = Integer.parseInt(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT, SolaceSparkStreamingProperties.OAUTH_CLIENT_TOKEN_FETCH_TIMEOUT_DEFAULT));
                    boolean validateSSLCertificate = Boolean.parseBoolean(properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_SSL_VALIDATE_CERTIFICATE, "true"));
                    String trustStoreFilePath = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_FILE, null);
                    String trustStoreFilePassword = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_PASSWORD, null);
                    String trustStoreType = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_TYPE, SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TRUSTSTORE_TYPE_DEFAULT);
                    String tlsVersion = properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TLS_VERSION, SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_TLS_VERSION_DEFAULT);

                    oAuthClient = new OAuthClient(properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_URL), properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CLIENT_ID),
                            properties.get(SolaceSparkStreamingProperties.OAUTH_CLIENT_CREDENTIALS_CLIENTSECRET));
                    oAuthClient.buildRequest(fetchTimeout, properties.getOrDefault(SolaceSparkStreamingProperties.OAUTH_CLIENT_AUTHSERVER_CLIENT_CERTIFICATE, null), trustStoreFilePath, trustStoreFilePassword, tlsVersion, trustStoreType, validateSSLCertificate);

                    jcsmpProperties.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oAuthClient.getAccessToken().getValue());
                    scheduleOAuthRefresh(interval);
                }
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
                int reconnectRetryCount = Integer.parseInt(properties.get(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES));
                cp.setReconnectRetries(reconnectRetryCount);
            }
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST)) {
                cp.setConnectRetriesPerHost(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.SOLACE_CONNECT_RETRIES_PER_HOST)));
            }
            if(properties.containsKey(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME)) {
                cp.setReconnectRetryWaitInMillis(Integer.parseInt(properties.get(SolaceSparkStreamingProperties.SOLACE_RECONNECT_RETRIES_WAIT_TIME)));
            }
//            jcsmpProperties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, cp);
            session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties, null, new SolaceSessionEventListener(this));
            session.connect();
        } catch (Exception ex) {
            log.error("SolaceSparkConnector - Exception connecting to Solace ", ex);
            close();
            this.isException = true;
            this.exception = ex;
            throw new SolaceSessionException(ex);
        }
    }

    public void addReceiver(EventListener eventListener) {
        eventListeners.add(eventListener);
        setReceiver(eventListener);
    }

    private void setReceiver(EventListener eventListener) {
        try {
            ConsumerFlowProperties flowProp = new ConsumerFlowProperties();
            Queue listenQueue = JCSMPFactory.onlyInstance().createQueue(this.queue);

            flowProp.setEndpoint(listenQueue);
            flowProp.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
            EndpointProperties endpointProps = new EndpointProperties();
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

            eventListener.setBrokerInstance(this);
            FlowReceiver cons = this.session.createFlow(eventListener,
                    flowProp, endpointProps);

            cons.start();
            log.info("SolaceSparkConnector - Consumer flow started to listen for messages on queue {} ", this.queue);
            flowReceivers.add(cons);
        } catch (Exception e) {
            handleException("SolaceSparkConnector - Consumer received exception. Shutting down consumer ", e);
        }
    }

    public void initProducer(JCSMPStreamingPublishCorrelatingEventHandler jcsmpStreamingPublishCorrelatingEventHandler) {
        try {
            this.producer = this.session.getMessageProducer(jcsmpStreamingPublishCorrelatingEventHandler);
        } catch (JCSMPException e) {
            log.error("SolaceSparkConnector - Error creating publisher to Solace", e);
            throw new RuntimeException(e);
        }
    }

    public void publishMessage(String applicationMessageId, String topic, String partitionKey, Object msg, long timestamp, UnsafeMapData headersMap) {
        Map<String, Object> headers = new HashMap<>();
        if(headersMap != null && headersMap.numElements() > 0) {
            for (int i = 0; i < headersMap.numElements(); i++) {
                headers.put(headersMap.keyArray().get(i, DataTypes.StringType).toString(),
                        headersMap.valueArray().get(i, DataTypes.BinaryType));
            }
        }
        if(partitionKey != null && !partitionKey.isEmpty()) {
            headers.put(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY, partitionKey);
        }
        try {
            XMLMessage xmlMessage = SolaceUtils.map(msg, headers, UUID.randomUUID(), new ArrayList<>(), false);
//            xmlMessage.writeBytes(msg.toString().getBytes(StandardCharsets.UTF_8));
//            xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
            xmlMessage.setCorrelationKey(applicationMessageId);
            xmlMessage.setCorrelationId(applicationMessageId);
            xmlMessage.setApplicationMessageId(applicationMessageId);
            if(timestamp > 0L) {
                xmlMessage.setSenderTimestamp(timestamp);
            }
            xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
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

        if(scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            log.info("SolaceSparkConnector - OAuth token refresh thread is now shutdown");
        }
    }

    public ConcurrentLinkedQueue<SolaceMessage> getMessages(int index) {
        log.info("Requesting messages from event listener {}, total messages available :: {}", index, this.eventListeners.get(index).getMessages().size());
        return index < this.eventListeners.size() ? this.eventListeners.get(index).getMessages() : null;
    }

    @Override
    protected void finalize() {
        close();
    }

//    public boolean preReconnect() {
//        log.info("SolaceSparkConnector - Pre reconnect to Solace session");
//        try {
//            if (session != null) {
//                if(oAuthClient != null) {
//                    session.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oAuthClient.getAccessToken().getValue());
//                } else if(accessTokenSource != null) {
//                    String accessToken = readAccessTokenFromFile(accessTokenSource);
//                    session.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, accessToken);
//                }
//                log.info("SolaceSparkConnector - Updated Solace Session with new access token during pre-reconnect state");
//            }
//            reconnectCount++;
//            return true;
//        } catch (Exception e) {
//            reconnectCount--;
//            handleException("SolaceSparkConnector - Exception reconnecting to Solace ", e);
//            return false;
////            log.error("SolaceSparkConnector - Exception reconnecting to Solace ", e);
////            close();
////            throw new RuntimeException(e);
//        }
//    }
//
//    public void postReconnect() {
//        log.info("SolaceSparkConnector - Post reconnect to Solace session successful");
//        reconnectCount--;
//    }

    private void scheduleOAuthRefresh(int refreshInterval) {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if(session != null && oAuthClient != null) {
                try {
                    session.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oAuthClient.getAccessToken().getValue());
                    log.info("SolaceSparkConnector - Updated Solace Session with new access token received from OAuth Server");
                } catch (JCSMPException e) {
                    handleException("SolaceSparkConnector - Exception updating access token", e);
                }
            }
        }, 0, refreshInterval, TimeUnit.SECONDS);
    }

    private void scheduleOAuthRefresh(String accessTokenSource, int refreshInterval) {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if(session != null) {
                try {
                    String accessToken = readAccessTokenFromFile(accessTokenSource);
                    session.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, accessToken);
                    log.info("SolaceSparkConnector - Updated Solace Session with new access token retrieved from Access Token File {} ", accessTokenSource);
                } catch (Exception e) {
                    handleException("SolaceSparkConnector - Exception updating access token", e);
                }
            }
        }, 0, refreshInterval, TimeUnit.SECONDS);
    }

    private String readAccessTokenFromFile(String accessTokenSource) {
        try {
            File accessTokenFile = new File(accessTokenSource);
            if(accessTokenFile.lastModified() <= accessTokenSourceLastModifiedTime) {
                isAccessTokenSourceModified = false;
                log.info("SolaceSparkConnector - Access Token file is not modified since last read");
            } else if(accessTokenFile.lastModified() > accessTokenSourceLastModifiedTime) {
                isAccessTokenSourceModified = true;
            }
            accessTokenSourceLastModifiedTime = accessTokenFile.lastModified();
            List<String> accessTokens = Files.readAllLines(accessTokenFile.toPath());
            if (accessTokens.size() != 1) {
                log.error("SolaceSparkConnector - File {} is empty or has more than one access token", accessTokenSource);
                this.isException = true;
                this.exception = new SolaceInvalidAccessTokenException("SolaceSparkConnector - File " + accessTokenSource + " is empty or has more than one access token");
                throw exception;
            }
            return accessTokens.get(0);
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception when reading access token file", e);
            close();
            this.isException = true;
            this.exception = e;
            throw new SolaceInvalidAccessTokenException(e);
        }
    }

    public void handleException(String message, Exception e) {
        log.info("SolaceSparkConnector - Exception handling condition isOAuth {}, isFileModified {}", isOAuth, isAccessTokenSourceModified);
        if( !isOAuth || !isAccessTokenSourceModified || e.getCause().toString().contains("Unauthorized")) {
            log.error(message, e);
            this.isException = true;
            this.exception = e;
            close();
            throw new SolaceSessionException(e);
        }
    }

    public boolean isException() {
        return isException;
    }

    public Exception getException() {
        return exception;
    }
}
