package com.solacecoe.connectors.spark;

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.io.Serializable;

@FunctionalInterface
public interface InboundMessageMapper<T> extends Serializable {
    T map(BytesXMLMessage message) throws Exception;
}
