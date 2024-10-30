package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceInvalidAccessTokenException;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceSessionException;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkHeaders;
import com.solacecoe.connectors.spark.streaming.properties.SolaceHeaderMeta;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkHeadersMeta;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceSessionEventListener;
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class SolaceBroker implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceBroker.class);
    private final JCSMPSession session;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceBroker implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolaceBroker.class);
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

    public void publishMessage(String topic, Object msg, UnsafeMapData headersMap) {
        Map<String, Object> headers = new HashMap<>();
        for(int i=0; i< headersMap.numElements(); i++) {
//            System.out.println(headersMap.keyArray().get(i, DataTypes.StringType));
//            System.out.println(headersMap.valueArray().get(i, DataTypes.BinaryType));
            headers.put(headersMap.keyArray().get(i, DataTypes.StringType).toString(),
                    headersMap.valueArray().get(i, DataTypes.BinaryType));
        }

        try {
            XMLMessage xmlMessage = map(msg, headers, UUID.randomUUID(), new ArrayList<>(), false);
//            xmlMessage.writeBytes(msg.toString().getBytes(StandardCharsets.UTF_8));
//            xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
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

    private XMLMessage map(Object payload,
                   Map<String, Object> headers,
                   UUID messageId,
                   Collection<String> excludedHeaders,
                   boolean convertNonSerializableHeadersToString) throws SDTException {
        XMLMessage xmlMessage;
        SDTMap metadata = map(headers, excludedHeaders, convertNonSerializableHeadersToString);
//        rethrowableCall(metadata::putInteger, SolaceBinderHeaders.MESSAGE_VERSION, MESSAGE_VERSION);

        if (payload instanceof byte[]) {
            BytesMessage bytesMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            bytesMessage.setData((byte[]) payload);
            xmlMessage = bytesMessage;
        } else if (payload instanceof String) {
            TextMessage textMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
            textMessage.setText((String) payload);
            xmlMessage = textMessage;
        } else if (payload instanceof SDTStream) {
            StreamMessage streamMessage = JCSMPFactory.onlyInstance().createMessage(StreamMessage.class);
            streamMessage.setStream((SDTStream) payload);
            xmlMessage = streamMessage;
        } else if (payload instanceof SDTMap) {
            MapMessage mapMessage = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
            mapMessage.setMap((SDTMap) payload);
            xmlMessage = mapMessage;
        } else if (payload instanceof Serializable) {
            BytesMessage bytesMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            bytesMessage.setData(SerializationUtils.serialize((Serializable) payload));
            metadata.putBoolean(SolaceSparkHeaders.SERIALIZED_PAYLOAD, true);
            xmlMessage = bytesMessage;
        } else {
            String msg = String.format(
                    "Invalid payload received. Expected %s. Received: %s",
                    String.join(", ",
                            byte[].class.getSimpleName(),
                            String.class.getSimpleName(),
                            SDTStream.class.getSimpleName(),
                            SDTMap.class.getSimpleName(),
                            Serializable.class.getSimpleName()
                    ), payload.getClass().getName());
//            SolaceMessageConversionException exception = new SolaceMessageConversionException(msg);
//            LOGGER.warn(msg, exception);
            throw new RuntimeException(msg);
        }
        Object contentType = headers.get("content-type");
        if (contentType != null) {
            xmlMessage.setHTTPContentType(contentType.toString());
        }
        for (Map.Entry<String, SolaceHeaderMeta<?>> header : SolaceHeaderMeta.META.entrySet()) {
            if (!header.getValue().isWritable()) {
                continue;
            }

            Object value = headers.get(header.getKey());
            if (value != null) {
                if (!header.getValue().getType().isInstance(value)) {
                    if(Long.class.isAssignableFrom(header.getValue().getType())) {
                        value = Long.parseLong(new String((byte[]) value, StandardCharsets.UTF_8));
                        header.getValue().getWriteAction().accept(xmlMessage, value);
                    } else if(Integer.class.isAssignableFrom(header.getValue().getType())) {
                        value = Integer.parseInt(new String((byte[]) value, StandardCharsets.UTF_8));
                        header.getValue().getWriteAction().accept(xmlMessage, value);
                    } else if(Boolean.class.isAssignableFrom(header.getValue().getType())) {
                        value = Boolean.parseBoolean(new String((byte[]) value, StandardCharsets.UTF_8));
                        header.getValue().getWriteAction().accept(xmlMessage, value);
                    } else if(String.class.isAssignableFrom(header.getValue().getType())) {
                        value = new String((byte[]) value, StandardCharsets.UTF_8);
                        header.getValue().getWriteAction().accept(xmlMessage, value);
                    } else {
                        String msg = String.format(
                                "Message %s has an invalid value type for header %s. Expected %s but received %s.",
                                messageId, header.getKey(), header.getValue().getType(),
                                value.getClass());
                        throw new RuntimeException(msg);
                    }
                }
            } else if (header.getValue().hasOverriddenDefaultValue()) {
                value = header.getValue().getDefaultValueOverride();
            } else {
                continue;
            }

            try {
                header.getValue().getWriteAction().accept(xmlMessage, value);
            } catch (Exception e) {
                String msg = String.format("Could not set %s property from header %s of message %s",
                        XMLMessage.class.getSimpleName(), header.getKey(), messageId);
                throw new RuntimeException(msg, e);
            }
        }

        xmlMessage.setProperties(metadata);
        xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
        return xmlMessage;
    }

    private SDTMap map(Map<String, Object> headers, Collection<String> excludedHeaders, boolean convertNonSerializableHeadersToString) throws SDTException {
        SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
        Set<String> serializedHeaders = new HashSet<>();
        for (Map.Entry<String,Object> header : headers.entrySet()) {
            if (header.getKey().equalsIgnoreCase(SolaceSparkHeaders.CONFIRM_CORRELATION) ||
                    SolaceHeaderMeta.META.containsKey(header.getKey()) || SolaceSparkHeadersMeta.META.containsKey(header.getKey())) {
                continue;
            }
            if (excludedHeaders != null && excludedHeaders.contains(header.getKey())) {
                continue;
            }

            addSDTMapObject(metadata, serializedHeaders, header.getKey(), header.getValue(),
                    convertNonSerializableHeadersToString);
        }

        if (headers.containsKey(SolaceSparkHeaders.PARTITION_KEY)) {
            Object partitionKeyObj = headers.get(SolaceSparkHeaders.PARTITION_KEY);
            if (partitionKeyObj instanceof String) {
//                rethrowableCall(metadata::putString,
//                        XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY,
//                        partitionKey);
                metadata.putString(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY, (String) partitionKeyObj);
            } else {
                String msg = String.format("Incorrect type specified for header '%s'. Expected [%s] but actual type is [%s]",
                        SolaceSparkHeaders.PARTITION_KEY, String.class, partitionKeyObj.getClass());
//                SolaceMessageConversionException exception = new SolaceMessageConversionException(
//                        new IllegalArgumentException(msg));
//                LOGGER.warn(msg, exception);
                throw new RuntimeException(msg);
            }
        }

        if (!serializedHeaders.isEmpty()) {
//            rethrowableCall(metadata::putString, SolaceBinderHeaders.SERIALIZED_HEADERS,
//                    rethrowableCall(stringSetWriter::writeValueAsString, serializedHeaders));
//            metadata.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, );
//            rethrowableCall(metadata::putString, SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING,
//                    DEFAULT_ENCODING.getName());
        }
        return metadata;
    }

    private void addSDTMapObject(SDTMap sdtMap, Set<String> serializedHeaders, String key, Object object,
                                 boolean convertNonSerializableHeadersToString)
            throws RuntimeException, SDTException {
        try {
            sdtMap.putObject(key, object);
        } catch (IllegalArgumentException | SDTException e) {
//            if (object instanceof Serializable) {
////                rethrowableCall(sdtMap::putString, k,
////                        DEFAULT_ENCODING.encode(rethrowableCall(SerializationUtils::serialize, o)));
//                sdtMap.putString(key, Base64.Encoder.encode(SerializationUtils.serialize(object)));
//
//                serializedHeaders.add(k);
//            } else
            if (convertNonSerializableHeadersToString && object != null) {
//                LOGGER.debug("Irreversibly converting header {} to String", k);
                sdtMap.putString(key, object.toString());
            } else {
                throw e;
            }
        }
    }
}
