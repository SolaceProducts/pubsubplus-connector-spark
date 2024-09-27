package com.solacecoe.connectors.spark.streaming.solace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceConnectionManager implements Serializable {
    private static Logger log = LoggerFactory.getLogger(SolaceConnectionManager.class);
    private final CopyOnWriteArrayList<SolaceBroker> brokerConnections;
    private final CopyOnWriteArrayList<SolaceBroker> lvqConnections;

    public SolaceConnectionManager() {
        brokerConnections = new CopyOnWriteArrayList<>();
        lvqConnections = new CopyOnWriteArrayList<>();
    }

    public void addConnection(SolaceBroker solaceBroker) {
        this.brokerConnections.add(solaceBroker);
    }

    public void addLVQConnection(SolaceBroker solaceBroker) {
        this.lvqConnections.add(solaceBroker);
    }

    public SolaceBroker getConnection(int index) {
        return index < this.brokerConnections.size() ? this.brokerConnections.get(index) : null;
    }

    public CopyOnWriteArrayList<SolaceBroker> getConnections() {
        return brokerConnections;
    }

    public void close() {
        brokerConnections.forEach(SolaceBroker::close);
        lvqConnections.forEach(SolaceBroker::close);
    }
}
