package com.solacecoe.connectors.spark.streaming.solace.utils;

import com.solacecoe.connectors.spark.streaming.properties.SolaceHeaderMeta;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkHeaders;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkHeadersMeta;
import com.solacesystems.jcsmp.*;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SolaceUtils {

    public static String getMessageID(BytesXMLMessage message, String solaceOffsetIndicator) throws SDTException {
        switch (solaceOffsetIndicator) {
            case "CORRELATION_ID":
                if(message.getCorrelationId() == null || message.getCorrelationId().isEmpty()) {
                    throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator CORRELATION_ID is null or empty");
                }
                return message.getCorrelationId();
            case "APPLICATION_MESSAGE_ID":
                if(message.getApplicationMessageId() == null || message.getApplicationMessageId().isEmpty()) {
                    throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator APPLICATION_MESSAGE_ID is null or empty");
                }
                return message.getApplicationMessageId();
//            case "SEQUENCE_NUMBER":
//                return Long.toString(message.getSequenceNumber());
            default:
                if(solaceOffsetIndicator.equals("MESSAGE_ID")) {
                    return message.getReplicationGroupMessageId().toString();
                }

                if(message.getProperties() != null && message.getProperties().containsKey(solaceOffsetIndicator)) {
                    if(message.getProperties().get(solaceOffsetIndicator) == null || message.getProperties().get(solaceOffsetIndicator).toString().isEmpty()) {
                        throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator " + solaceOffsetIndicator + " is null or empty");
                    }
                    return message.getProperties().get(solaceOffsetIndicator).toString();
                } else {
                    throw new RuntimeException("SolaceSparkConnector - Unable to find Solace Offset Indicator in message headers");
                }
        }
    }

    public static XMLMessage map(Object payload,
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

    private static SDTMap map(Map<String, Object> headers, Collection<String> excludedHeaders, boolean convertNonSerializableHeadersToString) throws SDTException {
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

    private static void addSDTMapObject(SDTMap sdtMap, Set<String> serializedHeaders, String key, Object object,
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
