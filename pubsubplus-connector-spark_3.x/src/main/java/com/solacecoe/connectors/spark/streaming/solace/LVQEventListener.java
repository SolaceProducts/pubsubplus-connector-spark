package com.solacecoe.connectors.spark.streaming.solace;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkOffset;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;


public class LVQEventListener implements XMLMessageListener, Serializable {
    private static final Logger log = LogManager.getLogger(LVQEventListener.class);
    private final SolaceSparkOffset[] solaceSparkOffsets = new SolaceSparkOffset[1];
    @Override
    public void onReceive(BytesXMLMessage msg) {
        try {
            byte[] msgData = new byte[0];
            if (msg.getContentLength() != 0) {
                msgData = msg.getBytes();
            }
            if (msg.getAttachmentContentLength() != 0) {
                msgData = msg.getAttachmentByteBuffer().array();
            }
            JsonObject lastKnownOffset = new Gson().fromJson(new String(msgData, StandardCharsets.UTF_8), JsonObject.class);
            SolaceSparkOffset solaceSparkOffset = new SolaceSparkOffset(lastKnownOffset.get("offset").getAsInt(), lastKnownOffset.get("messageIDs").getAsString());
            log.info("SolaceSparkConnector - Received SolaceSparkOffset from LVQ with messageIDs {}", new String(msgData, StandardCharsets.UTF_8));
            this.solaceSparkOffsets[0] = solaceSparkOffset;
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception connecting to Solace Queue", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void onException(JCSMPException e) {
        log.error("SolaceSparkConnector - Consumer received exception: %s%n", e);
        throw new RuntimeException(e);
    }

    public SolaceSparkOffset[] getSolaceSparkOffsets() {
        return solaceSparkOffsets;
    }
}