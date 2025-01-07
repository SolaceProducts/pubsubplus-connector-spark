package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidAccessTokenException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceSessionException;
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class SolaceBroker implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceBroker.class);
    private final JCSMPSession session;
    private final String queue;
    private OAuthClient oAuthClient;
    private final CopyOnWriteArrayList<EventListener> eventListeners;
    private final CopyOnWriteArrayList<FlowReceiver> flowReceivers;
    private ScheduledExecutorService scheduledExecutorService;
    private boolean isException;
    private Exception exception;
    private long accessTokenSourceLastModifiedTime = 0L;
    private boolean isAccessTokenSourceModified = true;
    private boolean isOAuth = false;
    private final Map<String, String> properties;
    public SolaceBroker(Map<String, String> properties, boolean initiateReplay) {
        eventListeners = new CopyOnWriteArrayList<>();
        flowReceivers = new CopyOnWriteArrayList<>();
        this.properties = properties;
        this.initiateReplay = initiateReplay;
        this.queue = properties.get(SolaceSparkStreamingProperties.QUEUE);
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

            jcsmpProperties.setProperty(JCSMPProperties.HOST, properties.get(SolaceSparkStreamingProperties.HOST));            // host:port
            jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, properties.get(SolaceSparkStreamingProperties.VPN));    // message-vpn
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
                jcsmpProperties.setProperty(JCSMPProperties.USERNAME, properties.get(SolaceSparkStreamingProperties.USERNAME)); // client-username
                jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, properties.get(SolaceSparkStreamingProperties.PASSWORD)); // client-password
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
            session = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
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
                        if(replicationGroupMsgId != null) {
                            replayStart = JCSMPFactory.onlyInstance().createReplicationGroupMessageId(replicationGroupMsgId);
                        }
                        break;
                    default:
                        throw new RuntimeException("Unsupported replay strategy: " + replayStrategy);
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

    public void close() {
        flowReceivers.forEach(flowReceiver -> {
            if(flowReceiver != null && !flowReceiver.isClosed()) {
                String endpoint = flowReceiver.getEndpoint().getName();
                flowReceiver.close();
                log.info("SolaceSparkConnector - Closed flow receiver to endpoint {}", endpoint);
            }
        });
        flowReceivers.clear();
        eventListeners.clear();


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
