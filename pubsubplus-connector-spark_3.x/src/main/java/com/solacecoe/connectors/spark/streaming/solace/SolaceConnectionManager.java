package com.solacecoe.connectors.spark.streaming.solace;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceConnectionManager {
    private final static Logger logger = LogManager.getLogger(SolaceConnectionManager.class);
    private final static CopyOnWriteArrayList<SolaceBroker> brokerConnections = new CopyOnWriteArrayList<>();;

    public static void addConnection(SolaceBroker solaceBroker) {
        brokerConnections.add(solaceBroker);
    }

    public static SolaceBroker getConnection(int index) {
        return index < brokerConnections.size() ? brokerConnections.get(index) : null;
    }

    public CopyOnWriteArrayList<SolaceBroker> getConnections() {
        return brokerConnections;
    }

    public static void close() {
        logger.info("Closing connection manager for {} brokers ", brokerConnections.size());
        brokerConnections.forEach(SolaceBroker::close);
    }
}
