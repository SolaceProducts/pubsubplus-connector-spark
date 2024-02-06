package com.solacecoe.connectors.spark.streaming.solace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceConnectionManager implements Serializable {
    private static Logger log = LoggerFactory.getLogger(SolaceConnectionManager.class);
    private final CopyOnWriteArrayList<SolaceBroker> brokerConnections;

    public SolaceConnectionManager() {
        brokerConnections = new CopyOnWriteArrayList<>();
    }

    public void addConnection(SolaceBroker solaceBroker) {
        this.brokerConnections.add(solaceBroker);
    }

    public SolaceBroker getConnection(int index) {
        return index < this.brokerConnections.size() ? this.brokerConnections.get(index) : null;
    }

    public CopyOnWriteArrayList<SolaceBroker> getConnections() {
        return brokerConnections;
    }

    public void close() {
        brokerConnections.stream().forEach(brokerConnections -> brokerConnections.close());
    }
}
