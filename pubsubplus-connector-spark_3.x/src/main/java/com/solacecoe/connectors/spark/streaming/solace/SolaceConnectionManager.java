package com.solacecoe.connectors.spark.streaming.solace;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.util.ShutdownHookManager;
import scala.runtime.BoxedUnit;

import java.util.concurrent.ConcurrentHashMap;

public class SolaceConnectionManager {
    private static final Logger logger = LogManager.getLogger(SolaceConnectionManager.class);
    private static final ConcurrentHashMap<Integer, SolaceBroker> brokerConnections = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(SolaceConnectionManager::close));
        ShutdownHookManager.addShutdownHook(() -> {
            close();
            return BoxedUnit.UNIT;
        });
    }

    public static void addConnection(int index, SolaceBroker solaceBroker) {
        brokerConnections.put(index, solaceBroker);
    }

    public static SolaceBroker getConnection(int index) {
        return brokerConnections.getOrDefault(index, null);
    }

    public static void close() {
        logger.info("SolaceSparkConnector - Closing connection manager for {} brokers sessions", brokerConnections.size());
        brokerConnections.forEach((id, broker) -> {
            logger.info("SolaceSparkConnector - Closing connection for broker session {}", broker.getUniqueName());
            broker.close();
        });
        brokerConnections.clear();
    }
}
