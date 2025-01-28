package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.offset.SolaceMessageTracker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.util.ShutdownHookManager;
import scala.runtime.BoxedUnit;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SolaceConnectionManager {
    private static final Logger logger = LogManager.getLogger(SolaceConnectionManager.class);
    private static final ConcurrentHashMap<String, SolaceBroker> brokerConnections = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(SolaceConnectionManager::close));
        ShutdownHookManager.addShutdownHook(() -> {
            close();
            return BoxedUnit.UNIT;
        });
    }

    public static void addConnection(String id, SolaceBroker solaceBroker) {
        brokerConnections.put(id, solaceBroker);
    }

    public static SolaceBroker getConnection(String id) {
        return brokerConnections.getOrDefault(id, null);
    }

    public static SolaceBroker getFirstConnection() {
        return brokerConnections.values().stream().filter(Objects::nonNull).findFirst().orElse(null);
    }

    public static void close() {
        logger.info("SolaceSparkConnector - Closing connection manager for {} brokers sessions", brokerConnections.size());
        brokerConnections.forEach((id, broker) -> {
            logger.info("SolaceSparkConnector - Closing connection for broker session {}", broker.getUniqueName());
            broker.close();
        });
        brokerConnections.clear();
        SolaceMessageTracker.reset();
    }
}
