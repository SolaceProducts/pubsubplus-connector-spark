package com.solace.connector.spark.streaming.solace;

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.io.Serializable;

public class SolaceMessage implements Serializable {
    public transient BytesXMLMessage bytesXMLMessage;

    public SolaceMessage(BytesXMLMessage bytesXMLMessage) {
        this.bytesXMLMessage = bytesXMLMessage;
    }
}
