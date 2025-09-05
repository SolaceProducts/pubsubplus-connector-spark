package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidAccessTokenException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceSessionException;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.*;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class SolaceBroker implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceBroker.class);
    private final String queue;
    private final String lvqName;
    private final String lvqTopic;
    private String uniqueName = "";
    private OAuthClient oAuthClient;
    private final CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<LVQEventListener> lvqEventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;
    private ScheduledExecutorService scheduledExecutorService;
    private boolean isException;
    private Exception exception;
    private long accessTokenSourceLastModifiedTime = 0L;
    private boolean isAccessTokenSourceModified = true;
    private boolean isOAuth = false;
    private final Map<String, String> properties;
    private final JCSMPSession session;
    private XMLMessageProducer producer;
    private long lastMessageTimestamp = 0;
    private boolean isShuttingDown = false;
    public SolaceBroker(Map<String, String> properties, String clientType) {
        eventListeners = new CopyOnWriteArrayList<>();
        flowReceivers = new CopyOnWriteArrayList<>();
        lvqEventListeners = new CopyOnWriteArrayList<>();
        this.properties = properties;
        this.lvqName = properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_NAME, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_NAME);
        this.lvqTopic = properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_TOPIC, SolaceSparkStreamingProperties.SOLACE_SPARK_CONNECTOR_LVQ_DEFAULT_TOPIC);
        this.queue = properties.getOrDefault(SolaceSparkStreamingProperties.QUEUE, "");
        try {
            JCSMPProperties jcsmpProperties = new JCSMPProperties();
            jcsmpProperties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 50); // default window size for publishing
            // get api properties
            Properties props = getProperties(properties);
            if(!props.isEmpty()) {
                jcsmpProperties = JCSMPProperties.fromProperties(props);
            }

            jcsmpProperties.setProperty(JCSMPProperties.HOST, properties.get(SolaceSparkStreamingProperties.HOST));            // host:port
            jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, properties.get(SolaceSparkStreamingProperties.VPN));    // message-vpn

            String authenticationScheme = properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX + JCSMPProperties.AUTHENTICATION_SCHEME, null);
            if(authenticationScheme != null && authenticationScheme.equals(JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2)) {
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
                jcsmpProperties.setProperty(JCSMPProperties.USERNAME, properties.getOrDefault(SolaceSparkStreamingProperties.USERNAME, "")); // client-username
                jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, properties.getOrDefault(SolaceSparkStreamingProperties.PASSWORD, "")); // client-password
            }

            this.uniqueName = JCSMPFactory.onlyInstance().createUniqueName("solace/spark/connector/"+clientType);
            jcsmpProperties.setProperty(JCSMPProperties.CLIENT_NAME, uniqueName);
            addChannelProperties(jcsmpProperties);
            session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
            session.connect();
            startWatchdog();
        } catch (Exception ex) {
            log.error("SolaceSparkConnector - Exception connecting to Solace ", ex);
            close();
            this.isException = true;
            this.exception = ex;
            throw new SolaceSessionException(ex);
        }
    }

    private void addChannelProperties(JCSMPProperties jcsmpProperties) {
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
    }

    private static @NotNull Properties getProperties(Map<String, String> properties) {
        Properties props = new Properties();
        for(Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX)) {
                String value = entry.getValue();
                String solaceKey = entry.getKey().substring(SolaceSparkStreamingProperties.SOLACE_API_PROPERTIES_PREFIX.length());
                props.put("jcsmp." + solaceKey, value);
            }
        }
        return props;
    }

    public void addReceiver(EventListener eventListener) {
        eventListeners.add(eventListener);
        setReceiver(eventListener);
    }

    private void setReceiver(EventListener eventListener) {
        try {
            ReplayStartLocation replayStart = null;
            String replayStrategy = this.properties.getOrDefault(SolaceSparkStreamingProperties.REPLAY_STRATEGY ,null);
            if(replayStrategy != null) {
                switch (replayStrategy) {
                    case "BEGINNING":
                        replayStart = JCSMPFactory.onlyInstance().createReplayStartLocationBeginning();
                        break;
                    case "TIMEBASED":
                        String dateStr = properties.getOrDefault(SolaceSparkStreamingProperties.REPLAY_STRATEGY_START_TIME ,null);
                        if (dateStr != null) {
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                            simpleDateFormat.setTimeZone(TimeZone.getTimeZone(properties.getOrDefault(SolaceSparkStreamingProperties.REPLAY_STRATEGY_TIMEZONE, "UTC"))); // Convert the given date into UTC time zone
                            Date date = simpleDateFormat.parse(dateStr);
                            replayStart = JCSMPFactory.onlyInstance().createReplayStartLocationDate(date);
                        }
                        break;
                    case "REPLICATION-GROUP-MESSAGE-ID":
                        String replicationGroupMsgId = this.properties.getOrDefault(SolaceSparkStreamingProperties.REPLAY_STRATEGY_REPLICATION_GROUP_MESSAGE_ID, null);
                        if(replicationGroupMsgId != null && !replicationGroupMsgId.isEmpty()) {
                            replayStart = JCSMPFactory.onlyInstance().createReplicationGroupMessageId(replicationGroupMsgId);
                        } else {
                            handleException("SolaceSparkConnector - Invalid replication group message id", null);
                        }
                        break;
                    default:
                        handleException("SolaceSparkConnector - Unsupported replay strategy: " + replayStrategy, null);
                }
            }
            ConsumerFlowProperties flowProp = new ConsumerFlowProperties();
            Queue listenQueue = JCSMPFactory.onlyInstance().createQueue(this.queue);

            flowProp.setEndpoint(listenQueue);
            if(replayStart != null) {
                flowProp.setReplayStartLocation(replayStart);
            }
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
            eventListener.setBrokerInstance(this);
            FlowReceiver cons = this.session.createFlow(eventListener,
                    flow_prop, endpoint_props);

            cons.start();
            flowReceivers.add(cons);
            log.info("SolaceSparkConnector - Consumer flow started to listen for messages on queue {}", this.queue);
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Consumer received exception. Shutting down consumer ", e);
            close();
        }
    }

    public void initProducer(JCSMPStreamingPublishCorrelatingEventHandler jcsmpStreamingPublishCorrelatingEventHandler) {
        try {
            this.producer = this.session.getMessageProducer(jcsmpStreamingPublishCorrelatingEventHandler);
        } catch (JCSMPException e) {
            log.error("SolaceSparkConnector - Error creating publisher to Solace", e);
            handleException("SolaceSparkConnector - Exception initializing producer ", e);
        }
    }

    public void initProducer() {
        try {
            this.producer = this.session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object o) {
                    log.info("SolaceSparkConnector - LVQ Message published successfully to Solace");
                }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) {
                    log.error("SolaceSparkConnector - Exception when publishing message to Solace", e);
                    handleException("SolaceSparkConnector - Exception publishing message to LVQ ", e);
                }
            });
        } catch (JCSMPException e) {
            log.error("SolaceSparkConnector - Error creating publisher to Solace", e);
            handleException("SolaceSparkConnector - Exception initializing producer ", e);
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
            log.error("SolaceSparkConnector - Exception publishing lvq message to Solace", e);
            handleException("SolaceSparkConnector - Exception publishing lvq message to Solace ", e);
        }
    }

    public void publishMessage(String applicationMessageId, String topic, String partitionKey, Object msg, long timestamp, UnsafeMapData headersMap) {
        try{
            Destination destination = JCSMPFactory.onlyInstance().createTopic(topic);
            XMLMessage xmlMessage = createMessage(applicationMessageId, partitionKey, msg, timestamp, headersMap);
            this.producer.send(xmlMessage, destination);
        } catch (JCSMPException e) {
            log.error("SolaceSparkConnector - Exception publishing message to Solace ", e);
            handleException("SolaceSparkConnector - Exception publishing message to Solace ", e);
        }
    }

    public XMLMessage createMessage(String applicationMessageId, String partitionKey, Object msg, long timestamp, UnsafeMapData headersMap) {
        Map<String, Object> headers = new HashMap<>();
        if(headersMap != null && headersMap.numElements() > 0) {
            for (int i = 0; i < headersMap.numElements(); i++) {
                headers.put(headersMap.keyArray().get(i, DataTypes.StringType).toString(),
                        headersMap.valueArray().get(i, DataTypes.BinaryType));
            }
        }

        XMLMessage xmlMessage = null;
        try {
            xmlMessage = SolaceUtils.map(msg, headers, applicationMessageId, new ArrayList<>(), false);
        } catch (SDTException e) {
            throw new RuntimeException(e);
        }

        if(partitionKey != null && !partitionKey.isEmpty()) {
            headers.put(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY, partitionKey);
        }
//            xmlMessage.writeBytes(msg.toString().getBytes(StandardCharsets.UTF_8));
//            xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
        xmlMessage.setCorrelationKey(applicationMessageId);
        xmlMessage.setCorrelationId(applicationMessageId);
        xmlMessage.setApplicationMessageId(applicationMessageId);
        if (timestamp > 0L) {
            xmlMessage.setSenderTimestamp(timestamp);
        }
        xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);

        return xmlMessage;
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
                flowReceiver.stop();
                flowReceiver.close();
                log.info("SolaceSparkConnector - Closed flow receiver to endpoint {}", endpoint);
            }
        });
        flowReceivers.clear();
        eventListeners.forEach(eventListener -> eventListener.getMessages().clear());
        eventListeners.clear();
        lvqEventListeners.clear();
    }

    public void close() {
        isShuttingDown = true;
        try {
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

        } catch (Exception e) {
            log.error("SolaceSparkConnector - Graceful shutdown failed", e);
            throw e;
        }
    }

    public boolean isConnected() {
        return session != null && !session.isClosed();
    }

    public LinkedBlockingQueue<SolaceMessage> getMessages(int index) {
        return index < this.eventListeners.size() ? this.eventListeners.get(index).getMessages() : null;
    }

    public CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> getOffsetFromLvq() {
        if(!lvqEventListeners.isEmpty() && !this.lvqEventListeners.get(0).getLastKnownOffsets().isEmpty()) {
            log.info("Requesting messages from lvq listener, total messages available :: {}", this.lvqEventListeners.get(0).getLastKnownOffsets().size());
            return this.lvqEventListeners.get(0).getLastKnownOffsets();
        }

        return new CopyOnWriteArrayList<>();
    }

    @Override
    protected void finalize() {
        close();
    }

    public String getUniqueName() {
        return uniqueName;
    }
    
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
            scheduledExecutorService.shutdown();
            throw new SolaceInvalidAccessTokenException(e);
        }
    }

    public void handleException(String message, Exception e) {
        log.info("SolaceSparkConnector - Exception handling condition isOAuth {}, isFileModified {}", isOAuth, isAccessTokenSourceModified);
        if( !isOAuth || !isAccessTokenSourceModified || (e != null && e.getCause().toString().contains("Unauthorized"))) {
            log.error(message, e);
            this.isException = true;
            this.exception = e;
            if(e == null) {
                throw new SolaceSessionException(message);
            } else {
                throw new SolaceSessionException(e);
            }
        }
    }

    public boolean isException() {
        return isException;
    }

    public Exception getException() {
        return exception;
    }

    private void startWatchdog() {
        long timeout = Long.parseLong(this.properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_CONNECTION_IDLE_TIMEOUT, SolaceSparkStreamingProperties.SOLACE_CONNECTION_IDLE_TIMEOUT_DEFAULT));
        long interval = Long.parseLong(this.properties.getOrDefault(SolaceSparkStreamingProperties.SOLACE_CONNECTION_IDLE_TIMEOUT_CHECK, SolaceSparkStreamingProperties.SOLACE_CONNECTION_IDLE_TIMEOUT_CHECK_DEFAULT));

        if(timeout > 0 && interval > 0) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                if (isShuttingDown) {
                    scheduler.shutdown();
                    return;
                }

                if (lastMessageTimestamp > 0 && (System.currentTimeMillis() - lastMessageTimestamp) > timeout) {
                    log.info("SolaceSparkConnector - Inactivity timeout. Last message processed at {} Shutting down Solace Session.", lastMessageTimestamp);
                    close();
                    scheduler.shutdown();
                } else {
                    log.info("SolaceSparkConnector - No messages are processed yet, skipping idle timeout check.");
                }
            }, interval, interval, TimeUnit.MILLISECONDS); // initial delay, then interval
        } else {
            log.info("SolaceSparkConnector - No connection idle timeout is configured. Connector will not check for idle connections.");
        }
    }

    public void setLastMessageTimestamp(long lastMessageTimestamp) {
        this.lastMessageTimestamp = lastMessageTimestamp;
    }
}
