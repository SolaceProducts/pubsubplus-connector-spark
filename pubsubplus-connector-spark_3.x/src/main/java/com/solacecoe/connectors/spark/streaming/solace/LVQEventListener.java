package com.solacecoe.connectors.spark.streaming.solace;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.solace.exceptions.SolaceConsumerException;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;


public class LVQEventListener implements XMLMessageListener, Serializable {
    private static final Logger log = LogManager.getLogger(LVQEventListener.class);
    private final transient SolaceSparkPartitionCheckpoint[] solaceSparkPartitionCheckpoints = new SolaceSparkPartitionCheckpoint[1];
    private transient CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> lastKnownOffset = new CopyOnWriteArrayList<>();
    private SolaceBroker solaceBroker;
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
            lastKnownOffset = new Gson().fromJson(new String(msgData, StandardCharsets.UTF_8), new TypeToken<CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint>>(){}.getType());
        } catch (Exception e) {
            log.error("SolaceSparkConnector - Exception connecting to Solace Queue", e);
            throw new SolaceConsumerException(e);
        }

    }

    public void setBrokerInstance(SolaceBroker solaceBroker) {
        this.solaceBroker = solaceBroker;
    }

    @Override
    public void onException(JCSMPException e) {
        if(solaceBroker != null) {
            solaceBroker.handleException("SolaceSparkConnector - Consumer received exception", e);
        } else {
            log.error("SolaceSparkConnector - Consumer received exception: %s%n", e);
            throw new SolaceConsumerException(e);
        }
    }

    public SolaceSparkPartitionCheckpoint[] getSolaceSparkOffsets() {
        return solaceSparkPartitionCheckpoints;
    }

    public CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> getLastKnownOffsets() {
        return lastKnownOffset;
    }
}
